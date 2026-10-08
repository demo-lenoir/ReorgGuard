// Package multirpc selects one verified HTTP provider for each canonical sweep.
// Provider agreement is never treated as evidence of chain consensus.
package multirpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"reorgguard/internal/backfill"
	"reorgguard/internal/crashprobe"
	"reorgguard/internal/model"
	"reorgguard/internal/reorg"
	"reorgguard/internal/rpc"
	"reorgguard/internal/store"
	"reorgguard/internal/telemetry"
)

var ErrNoEligible = errors.New("no eligible HTTP RPC endpoint")
var ErrIncompatible = errors.New("HTTP RPC endpoint incompatible with durable checkpoint")

type Source interface {
	backfill.Source
	reorg.Source
}
type Repository interface {
	backfill.Repository
	reorg.Repository
	Checkpoint(context.Context, uint64) (store.Checkpoint, error)
	CanonicalLogs(context.Context, uint64, uint64, uint64) ([]model.Log, error)
}
type Endpoint struct {
	ID     string
	Source Source
	// InitialRange optionally overrides the shared initial window for this
	// endpoint. Adaptive state is retained by its own Runner across sweeps.
	InitialRange uint64
}
type Config struct {
	ChainID      uint64
	Genesis      common.Hash
	Filter       rpc.Filter
	ProbeTimeout time.Duration
	Cooldown     time.Duration
	MaxSwitches  int
	Runner       *backfill.Runner
	Engine       *reorg.Engine
	Logger       *slog.Logger
}
type EndpointHealth struct {
	ID                   string
	State                string
	Reason               string
	ReportedChainID      uint64
	Head                 uint64
	CheckpointCompatible bool
	ConsecutiveFailures  uint64
	RecoverySuccesses    uint64
	TransportErrors      uint64
	Timeouts             uint64
	RateLimits           uint64
	LimitResponses       uint64
	Malformed            uint64
	Latency              time.Duration
	CooldownUntil        time.Time
	Selected             bool
	Backfill             backfill.ParallelSnapshot
	AdaptiveWindow       uint64
}
type Status struct {
	Active            string
	Failovers         uint64
	Incompatibilities uint64
	StaleDetections   uint64
	Failures          map[string]uint64
	FailoverReasons   map[string]uint64
	Endpoints         []EndpointHealth
}
type endpointState struct {
	health           EndpointHealth
	source           Source
	runner           *backfill.Runner
	engine           *reorg.Engine
	hardReject       bool
	checkpointReject *store.Checkpoint
	everCompatible   bool
}
type Pool struct {
	config            Config
	repo              Repository
	endpoints         []*endpointState
	mu                sync.Mutex
	sweepMu           sync.Mutex
	active            int
	failovers         uint64
	incompatibilities uint64
	stale             uint64
	failures          map[string]uint64
	failoverReasons   map[string]uint64
	lastReason        string
	requests          map[string]uint64
	latency           map[string]time.Duration
}

var endpointID = regexp.MustCompile(`^(primary|fallback_[1-9][0-9]*)$`)

func New(repo Repository, endpoints []Endpoint, cfg Config) (*Pool, error) {
	if repo == nil || len(endpoints) == 0 || len(endpoints) > 4 || cfg.ChainID == 0 || cfg.Genesis == (common.Hash{}) || cfg.ProbeTimeout <= 0 || cfg.Cooldown <= 0 || cfg.MaxSwitches < 1 || cfg.MaxSwitches > 16 || cfg.Filter.Validate() != nil || cfg.Runner == nil || cfg.Engine == nil {
		return nil, errors.New("invalid RPC pool configuration")
	}
	if cfg.Runner.ChainID != cfg.ChainID || cfg.Runner.Genesis != cfg.Genesis || cfg.Runner.Filter.Fingerprint() != cfg.Filter.Fingerprint() || cfg.Engine.ChainID != cfg.ChainID || cfg.Engine.Filter.Fingerprint() != cfg.Filter.Fingerprint() || (cfg.Runner.StartBlock > 1 && cfg.Runner.AnchorHash == (common.Hash{})) {
		return nil, errors.New("inconsistent RPC pool dataset configuration")
	}
	if cfg.Runner.Config.Validate() != nil {
		return nil, errors.New("invalid pool backfill configuration")
	}
	p := &Pool{config: cfg, repo: repo, active: -1, failures: make(map[string]uint64), failoverReasons: make(map[string]uint64), requests: make(map[string]uint64), latency: make(map[string]time.Duration)}
	seen := map[string]bool{}
	for i, ep := range endpoints {
		if ep.Source == nil || !endpointID.MatchString(ep.ID) || seen[ep.ID] || (i == 0 && ep.ID != "primary") || (i > 0 && ep.ID != fmt.Sprintf("fallback_%d", i)) {
			return nil, errors.New("invalid endpoint ID or source")
		}
		seen[ep.ID] = true
		s := &endpointState{health: EndpointHealth{ID: ep.ID, State: "recovering"}}
		s.source = &observedSource{pool: p, index: i, base: ep.Source}
		runner := &backfill.Runner{Source: s.source, Store: repo, Config: cfg.Runner.Config, ChainID: cfg.Runner.ChainID, Genesis: cfg.Runner.Genesis, AnchorHash: cfg.Runner.AnchorHash, StartBlock: cfg.Runner.StartBlock, Filter: cfg.Runner.Filter, OnRange: cfg.Runner.OnRange, Metrics: cfg.Runner.Metrics}
		if ep.InitialRange != 0 {
			runner.Config.InitialRange = ep.InitialRange
		}
		if runner.Config.Validate() != nil {
			return nil, errors.New("invalid endpoint range")
		}
		runner.Adaptive = &backfill.AdaptiveState{}
		runner.Parallel = &backfill.ParallelStats{}
		s.runner = runner
		engine := &reorg.Engine{Source: s.source, Store: repo, ChainID: cfg.Engine.ChainID, Filter: cfg.Engine.Filter, MaxDepth: cfg.Engine.MaxDepth, Timeout: cfg.Engine.Timeout, MaxLogsPerBlock: cfg.Engine.MaxLogsPerBlock, Logger: cfg.Engine.Logger, Metrics: cfg.Engine.Metrics}
		s.engine = engine
		p.endpoints = append(p.endpoints, s)
	}
	return p, nil
}
func (p *Pool) logger() *slog.Logger {
	if p.config.Logger != nil {
		return p.config.Logger
	}
	return slog.Default()
}
func (p *Pool) Status() Status {
	p.mu.Lock()
	defer p.mu.Unlock()
	x := Status{Failovers: p.failovers, Incompatibilities: p.incompatibilities, StaleDetections: p.stale, Failures: make(map[string]uint64, len(p.failures)), FailoverReasons: make(map[string]uint64, len(p.failoverReasons)), Endpoints: make([]EndpointHealth, len(p.endpoints))}
	for k, v := range p.failures {
		x.Failures[k] = v
	}
	for k, v := range p.failoverReasons {
		x.FailoverReasons[k] = v
	}
	for i, s := range p.endpoints {
		x.Endpoints[i] = s.health
		x.Endpoints[i].Selected = i == p.active
		x.Endpoints[i].Backfill = s.runner.Parallel.Snapshot()
		x.Endpoints[i].AdaptiveWindow, _ = s.runner.Adaptive.Snapshot()
	}
	if p.active >= 0 {
		x.Active = p.endpoints[p.active].health.ID
	}
	return x
}

// Sweep pins one endpoint for every RPC in a complete coordinator pass.
// A failed pass may already have committed preceding ranges; the next pass
// re-reads PostgreSQL and resumes at exactly its durable checkpoint + 1.
func (p *Pool) Sweep(ctx context.Context) (backfill.Result, uint64, error) {
	ctx, span := telemetry.Start(ctx, "provider.sweep", attribute.Int("provider.count", len(p.endpoints)))
	defer span.End()
	p.sweepMu.Lock()
	defer p.sweepMu.Unlock()
	var last error
	attempted := make(map[int]bool, len(p.endpoints))
	for attempt := 0; attempt < p.config.MaxSwitches; attempt++ {
		checkpoint, exists, err := p.checkpoint(ctx)
		if err != nil {
			return backfill.Result{}, 0, err
		}
		i, head, err := p.selectEndpoint(ctx, checkpoint, exists, attempted)
		if err != nil {
			if ctx.Err() != nil {
				return backfill.Result{}, 0, ctx.Err()
			}
			return backfill.Result{}, 0, errors.Join(ErrNoEligible, last, err)
		}
		s := p.endpoints[i]
		attempted[i] = true
		coordinator := reorg.Coordinator{Backfill: s.runner, Engine: s.engine}
		result, runErr := coordinator.RunTo(ctx, head)
		if runErr == nil {
			p.markSuccess(i)
			// Lag is observational. Failure here does not undo a committed batch;
			// the next sweep will read the new head again.
			if latest, latestErr := s.source.BlockNumber(ctx); latestErr == nil {
				head = latest
			}
			return result, head, nil
		}
		if ctx.Err() != nil {
			return result, head, ctx.Err()
		}
		if unsafe(runErr) {
			return result, head, runErr
		}
		p.markFailure(i, runErr)
		if pauseErr := crashprobe.Pause(ctx, "during_failover"); pauseErr != nil {
			return result, head, pauseErr
		}
		last = runErr
	}
	return backfill.Result{}, 0, errors.Join(ErrNoEligible, last)
}
func (p *Pool) checkpoint(ctx context.Context) (store.Checkpoint, bool, error) {
	cp, err := p.repo.Checkpoint(ctx, p.config.ChainID)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.Checkpoint{}, false, nil
	}
	if err != nil {
		return store.Checkpoint{}, false, err
	}
	return cp, true, nil
}
func (p *Pool) selectEndpoint(ctx context.Context, cp store.Checkpoint, hasCP bool, attempted map[int]bool) (int, uint64, error) {
	var last error
	selected, selectedHead := -1, uint64(0)
	eligible := make(map[int]uint64, len(p.endpoints))
	for i, s := range p.endpoints {
		if ctx.Err() != nil {
			return -1, 0, ctx.Err()
		}
		p.mu.Lock()
		if s.checkpointReject != nil && (!hasCP || *s.checkpointReject != cp) {
			s.checkpointReject = nil
			s.health.State = "recovering"
			s.health.Reason = ""
		}
		skip := attempted[i] || s.hardReject || s.checkpointReject != nil || time.Now().Before(s.health.CooldownUntil)
		allowFork := p.active == i && s.everCompatible
		// After restart there is no active endpoint. A source that can still
		// prove the complete old checkpoint by hash may propose a bounded fork.
		allowFork = allowFork || p.active == -1
		p.mu.Unlock()
		if skip {
			continue
		}
		head, err := p.probe(ctx, i, cp, hasCP, allowFork)
		if err != nil {
			if ctx.Err() != nil {
				return -1, 0, ctx.Err()
			}
			last = err
			continue
		}
		eligible[i] = head
		if selected < 0 || head > selectedHead {
			selected, selectedHead = i, head
		}
	}
	if selected < 0 {
		p.mu.Lock()
		allHard := true
		for _, s := range p.endpoints {
			if !s.hardReject {
				allHard = false
				break
			}
		}
		p.mu.Unlock()
		if allHard {
			return -1, 0, errors.Join(ErrIncompatible, last)
		}
		return -1, 0, ErrNoEligible
	}
	p.mu.Lock()
	previous := p.active
	p.mu.Unlock()
	// The candidate is fully probed, but no in-memory switch has happened yet.
	// This isolated test hook lets the process harness kill at that boundary.
	if previous >= 0 && previous != selected {
		if err := crashprobe.Pause(ctx, "during_failover"); err != nil {
			return -1, 0, err
		}
	}
	p.mu.Lock()
	old := p.active
	reason := p.lastReason
	if reason == "" && old >= 0 && old != selected {
		reason = "head_lag"
	}
	if old != selected {
		if old >= 0 {
			p.failovers++
			p.failoverReasons[reason]++
		}
		p.active = selected
	}
	for i, head := range eligible {
		s := p.endpoints[i]
		s.health.CheckpointCompatible = true
		s.everCompatible = true
		if i == selected {
			s.health.State = "recovering"
			s.health.Reason = ""
		} else if head < selectedHead {
			if s.health.State != "stale" {
				p.stale++
			}
			s.health.State = "stale"
			s.health.Reason = "head_lag"
		}
	}
	p.lastReason = ""
	p.mu.Unlock()
	if old != selected && old >= 0 {
		p.logger().Info("HTTP RPC failover", "old_endpoint", p.endpoints[old].health.ID, "reason", reason, "candidate_endpoint", p.endpoints[selected].health.ID, "checkpoint_validated", true, "result", "selected")
	}
	return selected, selectedHead, nil
}
func (p *Pool) probe(ctx context.Context, i int, cp store.Checkpoint, hasCP, allowFork bool) (uint64, error) {
	work, cancel := context.WithTimeout(ctx, p.config.ProbeTimeout)
	defer cancel()
	s := p.endpoints[i]
	chain, err := s.source.ChainID(work)
	if err != nil {
		p.rejectProbe(i, err)
		return 0, err
	}
	p.mu.Lock()
	s.health.ReportedChainID = chain
	p.mu.Unlock()
	if chain != p.config.ChainID {
		p.rejectHard(i, "wrong_chain")
		return 0, ErrIncompatible
	}
	genesis, err := s.source.BlockByNumber(work, 0)
	if err != nil {
		p.rejectProbe(i, err)
		return 0, err
	}
	if genesis.Hash != p.config.Genesis {
		p.rejectHard(i, "wrong_genesis")
		return 0, ErrIncompatible
	}
	if p.config.Runner.StartBlock > 1 {
		anchor, anchorErr := s.source.BlockByNumber(work, p.config.Runner.StartBlock-1)
		if anchorErr != nil {
			p.rejectProbe(i, anchorErr)
			return 0, anchorErr
		}
		if anchor.Hash != p.config.Runner.AnchorHash {
			p.rejectHard(i, "wrong_anchor")
			return 0, ErrIncompatible
		}
	}
	head, err := s.source.BlockNumber(work)
	if err != nil {
		p.rejectProbe(i, err)
		return 0, err
	}
	p.mu.Lock()
	s.health.Head = head
	p.mu.Unlock()
	if hasCP {
		if head < cp.Number {
			p.mu.Lock()
			s.health.State = "stale"
			s.health.Reason = "behind_checkpoint"
			p.stale++
			p.mu.Unlock()
			return 0, ErrIncompatible
		}
		fork, verifyErr := backfill.VerifyCheckpoint(work, s.source, p.repo, p.config.ChainID, cp, p.config.Runner.StartBlock, p.config.Filter)
		if verifyErr != nil {
			if errors.Is(verifyErr, model.ErrIdentity) || errors.Is(verifyErr, backfill.ErrCheckpointHistory) {
				p.rejectCheckpoint(i, cp, "checkpoint_history_mismatch")
			} else {
				p.rejectProbe(i, verifyErr)
			}
			return 0, verifyErr
		}
		if fork && !allowFork {
			p.rejectCheckpoint(i, cp, "checkpoint_mismatch")
			return 0, ErrIncompatible
		}
	} else {
		byHash, hashErr := s.source.BlockByHash(work, p.config.Genesis)
		if hashErr != nil {
			p.rejectProbe(i, hashErr)
			return 0, hashErr
		}
		if byHash.Hash != p.config.Genesis {
			p.rejectHard(i, "genesis_history_mismatch")
			return 0, ErrIncompatible
		}
		var capabilityLogs []model.Log
		capabilityLogs, err = s.source.Logs(work, 0, 0, p.config.Filter)
		if err == nil {
			err = p.config.Filter.ValidateLogs(capabilityLogs)
		}
		if err != nil {
			p.rejectProbe(i, err)
			return 0, err
		}
	}
	return head, nil
}
func (p *Pool) rejectCheckpoint(i int, cp store.Checkpoint, reason string) {
	p.mu.Lock()
	p.endpoints[i].checkpointReject = &cp
	p.mu.Unlock()
	p.reject(i, reason, false)
}
func (p *Pool) rejectHard(i int, reason string) {
	p.reject(i, reason, true)
}
func (p *Pool) reject(i int, reason string, permanent bool) {
	p.mu.Lock()
	s := p.endpoints[i]
	if permanent {
		s.hardReject = true
	}
	s.health.State = "incompatible"
	s.health.Reason = reason
	s.health.CheckpointCompatible = false
	p.incompatibilities++
	if p.active == i {
		p.lastReason = reason
	}
	id := s.health.ID
	old := "none"
	if p.active >= 0 {
		old = p.endpoints[p.active].health.ID
	}
	p.mu.Unlock()
	p.logger().Warn("HTTP RPC candidate rejected", "old_endpoint", old, "reason", reason, "candidate_endpoint", id, "checkpoint_validated", false, "result", "rejected")
}
func (p *Pool) rejectProbe(i int, err error) {
	if missingMethod(err) {
		p.rejectHard(i, "missing_method")
		return
	}
	p.markFailure(i, err)
}
func missingMethod(err error) bool {
	var rpcErr *rpc.RPCError
	return errors.As(err, &rpcErr) && rpcErr.Code == -32601
}
func unsafe(err error) bool {
	return errors.Is(err, store.ErrUnsafe) || errors.Is(err, store.ErrCorruptCanonical) || errors.Is(err, store.ErrConfigMismatch) || errors.Is(err, model.ErrIdentity) || errors.Is(err, reorg.ErrDepth) || errors.Is(err, reorg.ErrAncestorMissing) || errors.Is(err, reorg.ErrParentChain)
}
func failureReason(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case rpc.IsRangeLimit(err), errors.Is(err, rpc.ErrRateLimited):
		return "rate_limit"
	case errors.Is(err, rpc.ErrMalformed), errors.Is(err, model.ErrInvalidRange):
		return "malformed"
	case missingMethod(err):
		return "missing_method"
	default:
		return "transport"
	}
}
func (p *Pool) markFailure(i int, err error) {
	reason := failureReason(err)
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.endpoints[i]
	s.health.ConsecutiveFailures++
	s.health.Reason = reason
	s.health.CooldownUntil = time.Now().Add(p.config.Cooldown)
	if p.active == i {
		p.lastReason = reason
	}
	switch reason {
	case "timeout":
		s.health.State = "temporarily_unavailable"
		s.health.Timeouts++
	case "rate_limit":
		s.health.State = "rate_limited"
		s.health.RateLimits++
	case "malformed":
		s.health.State = "degraded"
		s.health.Malformed++
	default:
		s.health.State = "temporarily_unavailable"
		s.health.TransportErrors++
	}
	p.failures[reason]++
}
func (p *Pool) markSuccess(i int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.endpoints[i]
	if s.health.ConsecutiveFailures > 0 {
		s.health.RecoverySuccesses++
	}
	s.health.ConsecutiveFailures = 0
	s.health.State = "healthy"
	s.health.Reason = ""
	s.health.CooldownUntil = time.Time{}
}

type observedSource struct {
	pool  *Pool
	index int
	base  Source
}

func (o *observedSource) observe(method string, start time.Time, err error) {
	o.pool.mu.Lock()
	defer o.pool.mu.Unlock()
	status := "ok"
	if err != nil {
		status = "error"
	}
	key := o.pool.endpoints[o.index].health.ID + "|" + method + "|" + status
	o.pool.requests[key]++
	o.pool.latency[o.pool.endpoints[o.index].health.ID+"|"+method] = time.Since(start)
	o.pool.endpoints[o.index].health.Latency = time.Since(start)
	if rpc.IsRangeLimit(err) || errors.Is(err, rpc.ErrRateLimited) {
		o.pool.endpoints[o.index].health.LimitResponses++
		o.pool.endpoints[o.index].health.State = "rate_limited"
		o.pool.endpoints[o.index].health.Reason = "provider_limit"
	}
}
func (o *observedSource) ChainID(ctx context.Context) (n uint64, err error) {
	start := time.Now()
	defer func() { o.observe("eth_chainId", start, err) }()
	return o.base.ChainID(ctx)
}
func (o *observedSource) BlockNumber(ctx context.Context) (n uint64, err error) {
	start := time.Now()
	defer func() { o.observe("eth_blockNumber", start, err) }()
	return o.base.BlockNumber(ctx)
}
func (o *observedSource) BlockByNumber(ctx context.Context, n uint64) (b model.Block, err error) {
	start := time.Now()
	defer func() { o.observe("eth_getBlockByNumber", start, err) }()
	return o.base.BlockByNumber(ctx, n)
}
func (o *observedSource) BlockByHash(ctx context.Context, h common.Hash) (b model.Block, err error) {
	start := time.Now()
	defer func() { o.observe("eth_getBlockByHash", start, err) }()
	return o.base.BlockByHash(ctx, h)
}
func (o *observedSource) Logs(ctx context.Context, from, to uint64, f rpc.Filter) (l []model.Log, err error) {
	start := time.Now()
	defer func() { o.observe("eth_getLogs", start, err) }()
	return o.base.Logs(ctx, from, to, f)
}
func (o *observedSource) LogsByBlockHash(ctx context.Context, h common.Hash, f rpc.Filter) (l []model.Log, err error) {
	start := time.Now()
	defer func() { o.observe("eth_getLogsByHash", start, err) }()
	return o.base.LogsByBlockHash(ctx, h, f)
}

// WritePrometheus emits only configured endpoint IDs, fixed RPC methods and
// enumerated states/reasons. It never includes provider URLs.
func (p *Pool) WritePrometheus(w io.Writer) error {
	status := p.Status()
	p.mu.Lock()
	requests := make(map[string]uint64, len(p.requests))
	latencies := make(map[string]time.Duration, len(p.latency))
	for k, v := range p.requests {
		requests[k] = v
	}
	for k, v := range p.latency {
		latencies[k] = v
	}
	p.mu.Unlock()
	if _, err := fmt.Fprintf(w, "reorgguard_rpc_failovers_total %d\nreorgguard_rpc_incompatibilities_total %d\nreorgguard_rpc_stale_detections_total %d\n", status.Failovers, status.Incompatibilities, status.StaleDetections); err != nil {
		return err
	}
	states := []string{"healthy", "degraded", "rate_limited", "stale", "incompatible", "temporarily_unavailable", "recovering"}
	methods := []string{"eth_chainId", "eth_blockNumber", "eth_getBlockByNumber", "eth_getBlockByHash", "eth_getLogs", "eth_getLogsByHash"}
	for _, e := range status.Endpoints {
		selected := 0
		if e.Selected {
			selected = 1
		}
		if _, err := fmt.Fprintf(w, "reorgguard_rpc_endpoint_selected{endpoint=%q} %d\nreorgguard_rpc_endpoint_head{endpoint=%q} %d\n", e.ID, selected, e.ID, e.Head); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "reorgguard_rpc_provider_limit_responses_total{endpoint=%q} %d\nreorgguard_rpc_adaptive_window_blocks{endpoint=%q} %d\n", e.ID, e.LimitResponses, e.ID, e.AdaptiveWindow); err != nil {
			return err
		}
		for _, state := range states {
			value := 0
			if e.State == state {
				value = 1
			}
			if _, err := fmt.Fprintf(w, "reorgguard_rpc_endpoint_state{endpoint=%q,state=%q} %d\n", e.ID, state, value); err != nil {
				return err
			}
		}
		for _, method := range methods {
			for _, result := range []string{"ok", "error"} {
				key := e.ID + "|" + method + "|" + result
				if _, err := fmt.Fprintf(w, "reorgguard_rpc_requests_total{endpoint=%q,method=%q,status=%q} %d\n", e.ID, method, result, requests[key]); err != nil {
					return err
				}
			}
			if _, err := fmt.Fprintf(w, "reorgguard_rpc_latency_seconds{endpoint=%q,method=%q} %g\n", e.ID, method, latencies[e.ID+"|"+method].Seconds()); err != nil {
				return err
			}
		}
		for _, metric := range []struct {
			name  string
			value int64
		}{{"active_backfill_workers", e.Backfill.Active}, {"inflight_backfill_ranges", e.Backfill.InFlight}, {"pending_ordered_results", e.Backfill.Pending}} {
			if _, err := fmt.Fprintf(w, "reorgguard_%s{endpoint=%q} %d\n", metric.name, e.ID, metric.value); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintf(w, "reorgguard_backfill_backpressure_total{endpoint=%q} %d\n", e.ID, e.Backfill.Backpressure); err != nil {
			return err
		}
	}
	for _, reason := range []string{"transport", "timeout", "rate_limit", "malformed", "head_lag", "wrong_chain", "wrong_genesis", "wrong_anchor", "checkpoint_mismatch", "checkpoint_log_mismatch", "missing_method"} {
		if _, err := fmt.Fprintf(w, "reorgguard_rpc_failover_reasons_total{reason=%q} %d\n", reason, status.FailoverReasons[reason]); err != nil {
			return err
		}
	}
	return nil
}
