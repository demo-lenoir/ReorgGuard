package reorg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"go.opentelemetry.io/otel/attribute"
	"reorgguard/internal/crashprobe"
	"reorgguard/internal/model"
	"reorgguard/internal/rpc"
	"reorgguard/internal/store"
	"reorgguard/internal/telemetry"
)

var ErrBusy = errors.New("reconciliation already in progress")
var ErrCandidateMoved = errors.New("remote candidate moved during reconciliation")

type Source interface {
	BlockByNumber(context.Context, uint64) (model.Block, error)
	BlockByHash(context.Context, common.Hash) (model.Block, error)
	LogsByBlockHash(context.Context, common.Hash, rpc.Filter) ([]model.Log, error)
}
type Repository interface {
	CanonicalHead(context.Context, uint64) (model.Block, error)
	CanonicalBlockByNumber(context.Context, uint64, uint64) (model.Block, error)
	Reconcile(context.Context, uint64, store.Checkpoint, store.Checkpoint, []model.BlockData, uint64) (store.ReorgResult, error)
	MarkUnsafe(context.Context, uint64, string) error
	Readiness(context.Context, uint64) (store.Readiness, error)
}
type Engine struct {
	Source          Source
	Store           Repository
	ChainID         uint64
	Filter          rpc.Filter
	MaxDepth        uint64
	Timeout         time.Duration
	MaxLogsPerBlock int
	Logger          *slog.Logger
	Metrics         *Metrics
	mu              sync.Mutex
	state           string
	reason          string
	terminal        bool
}

func (e *Engine) validate() error {
	if e.Source == nil || e.Store == nil || e.ChainID > math.MaxInt64 || e.MaxDepth == 0 || e.MaxDepth > 10000 || e.Timeout <= 0 || e.MaxLogsPerBlock < 1 {
		return errors.New("invalid reorg configuration")
	}
	return e.Filter.Validate()
}
func (e *Engine) start() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state == "reconciling" {
		return ErrBusy
	}
	if e.state == "unsafe" && e.terminal {
		return store.ErrUnsafe
	}
	e.state = "reconciling"
	e.reason = ""
	e.terminal = false
	return nil
}
func (e *Engine) healthy() {
	e.mu.Lock()
	e.state = "healthy"
	e.reason = ""
	e.terminal = false
	e.mu.Unlock()
}
func (e *Engine) failed(reason string, terminal bool) {
	e.mu.Lock()
	e.state = "unsafe"
	e.reason = reason
	e.terminal = terminal
	e.mu.Unlock()
	e.Metrics.Failure(reason)
}
func (e *Engine) logger() *slog.Logger {
	if e.Logger != nil {
		return e.Logger
	}
	return slog.Default()
}

// Readiness reports in-process reconciliation and durable fail-closed state.
func (e *Engine) Readiness(ctx context.Context) (store.Readiness, error) {
	db, err := e.Store.Readiness(ctx, e.ChainID)
	if err != nil {
		return store.Readiness{State: "unsafe", Reason: "database_failure"}, err
	}
	if db.State == "unsafe" {
		return db, nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state == "reconciling" {
		db.State = "reconciling"
	} else if e.state == "unsafe" {
		db.State = "unsafe"
		db.Reason = e.reason
	}
	return db, nil
}

// Reconcile proves and commits only the forked suffix through min(target, old
// head height). Later forward blocks are appended by the existing backfill path.
func (e *Engine) Reconcile(ctx context.Context, target uint64) (store.ReorgResult, error) {
	ctx, span := telemetry.Start(ctx, "canonical.reconcile", attribute.Int64("target.block", int64(target)))
	defer span.End()
	if err := e.validate(); err != nil {
		return store.ReorgResult{}, err
	}
	if err := e.start(); err != nil {
		return store.ReorgResult{}, err
	}
	work, cancel := context.WithTimeout(ctx, e.Timeout)
	defer cancel()
	old, err := e.Store.CanonicalHead(work, e.ChainID)
	if err != nil {
		return e.fail(work, "database_failure", err, false)
	}
	candidateHeight := target
	if candidateHeight > old.Number {
		candidateHeight = old.Number
	}
	candidate, err := e.Source.BlockByNumber(work, candidateHeight)
	if err != nil {
		return e.fail(work, "provider_inconsistent", err, false)
	}
	ancestorCtx, ancestorSpan := telemetry.Start(work, "canonical.find_ancestor", attribute.Int64("max.depth", int64(e.MaxDepth)))
	ancestor, branch, err := FindAncestor(ancestorCtx, old, candidate, e.MaxDepth,
		func(ctx context.Context, n uint64) (model.Block, error) {
			b, lookupErr := e.Store.CanonicalBlockByNumber(ctx, e.ChainID, n)
			if errors.Is(lookupErr, store.ErrBlockNotFound) {
				return model.Block{}, ErrAncestorMissing
			}
			return b, lookupErr
		},
		func(ctx context.Context, h common.Hash) (model.Block, error) {
			if pauseErr := crashprobe.Pause(ctx, "during_ancestor_search"); pauseErr != nil {
				return model.Block{}, pauseErr
			}
			return e.Source.BlockByHash(ctx, h)
		})
	if err != nil {
		reason, _ := reasonFor(err)
		telemetry.Failure(ancestorSpan, reason)
	}
	ancestorSpan.End()
	if err != nil {
		if errors.Is(err, ErrNoFork) {
			return e.fail(work, "provider_inconsistent", ErrCandidateMoved, false)
		}
		reason, persist := reasonFor(err)
		return e.fail(work, reason, err, persist)
	}
	blocks := make([]model.Block, len(branch))
	copy(blocks, branch)
	var allLogs []model.Log
	for _, b := range blocks {
		if pauseErr := crashprobe.Pause(work, "before_reorg_branch_fetch"); pauseErr != nil {
			return e.fail(work, "interrupted", pauseErr, false)
		}
		logs, fetchErr := e.Source.LogsByBlockHash(work, b.Hash, e.Filter)
		if fetchErr != nil {
			return e.fail(work, "provider_inconsistent", fetchErr, false)
		}
		if err := e.Filter.ValidateLogs(logs); err != nil {
			return e.fail(work, "provider_inconsistent", err, false)
		}
		if len(logs) > e.MaxLogsPerBlock {
			return e.fail(work, "provider_inconsistent", rpc.ErrResponseTooLarge, false)
		}
		allLogs = append(allLogs, logs...)
	}
	_, validationSpan := telemetry.Start(work, "canonical.validate_branch", attribute.Int("block.count", len(blocks)))
	items, err := model.BuildRange(blocks[0].Number, blocks[len(blocks)-1].Number, ancestor.Hash, blocks, allLogs)
	if err != nil {
		telemetry.Failure(validationSpan, "parent_chain_invalid")
	}
	validationSpan.End()
	if err != nil {
		return e.fail(work, "parent_chain_invalid", err, true)
	}
	latest, err := e.Source.BlockByNumber(work, candidateHeight)
	if err != nil {
		return e.fail(work, "provider_inconsistent", err, false)
	}
	if latest.Hash != candidate.Hash {
		return e.fail(work, "provider_inconsistent", ErrCandidateMoved, false)
	}
	result, err := e.Store.Reconcile(work, e.ChainID, store.Checkpoint{Number: old.Number, Hash: old.Hash}, store.Checkpoint{Number: ancestor.Number, Hash: ancestor.Hash}, items, e.MaxDepth)
	if err != nil {
		reason, persist := reasonFor(err)
		return e.fail(work, reason, err, persist)
	}
	e.Metrics.Success(result)
	e.healthy()
	e.logger().Info("canonical reorg committed", "chain_id", e.ChainID, "old_head_number", result.OldHead.Number, "old_head_hash", result.OldHead.Hash.Hex(), "ancestor_number", result.Ancestor.Number, "ancestor_hash", result.Ancestor.Hash.Hex(), "new_head_number", result.NewHead.Number, "new_head_hash", result.NewHead.Hash.Hex(), "depth", result.Depth, "orphaned_blocks", result.OrphanedBlocks, "orphaned_logs", result.OrphanedLogs, "recanonicalized_blocks", result.RecanonicalizedBlocks, "new_blocks", result.NewBlocks)
	return result, nil
}
func reasonFor(err error) (string, bool) {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "interrupted", false
	case errors.Is(err, ErrDepth), errors.Is(err, store.ErrDepth):
		return "depth_exceeded", true
	case errors.Is(err, ErrAncestorMissing), errors.Is(err, store.ErrBlockNotFound):
		return "ancestor_missing", true
	case errors.Is(err, ErrParentChain), errors.Is(err, model.ErrParent), errors.Is(err, store.ErrCorruptCanonical):
		return "parent_chain_invalid", true
	case errors.Is(err, model.ErrIdentity):
		return "identity_conflict", true
	case errors.Is(err, model.ErrInvalidRange):
		return "provider_inconsistent", true
	case errors.Is(err, ErrNoFork):
		return "provider_inconsistent", false
	case errors.Is(err, ErrRemoteLookup):
		return "provider_inconsistent", false
	default:
		return "database_failure", false
	}
}
func (e *Engine) fail(ctx context.Context, reason string, cause error, persist bool) (store.ReorgResult, error) {
	if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		reason = "interrupted"
		persist = false
	}
	e.failed(reason, persist)
	e.logger().Error("canonical reconciliation failed", "chain_id", e.ChainID, "reason", reason)
	if persist {
		// Use a fresh bounded context so a canceled fetch can never suppress a
		// durable fail-closed marker for a proven unsafe fork.
		markCtx, cancel := context.WithTimeout(context.Background(), e.Timeout)
		defer cancel()
		if err := e.Store.MarkUnsafe(markCtx, e.ChainID, reason); err != nil {
			return store.ReorgResult{}, errors.Join(cause, fmt.Errorf("persist unsafe state: %w", err))
		}
	}
	return store.ReorgResult{}, cause
}
