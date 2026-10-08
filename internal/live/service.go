// Package live runs one HTTP canonical writer, woken by untrusted WS hints or a poll.
package live

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"reorgguard/internal/backfill"
	"reorgguard/internal/model"
	"reorgguard/internal/multirpc"
	"reorgguard/internal/reorg"
	"reorgguard/internal/rpc"
	"reorgguard/internal/store"
	"reorgguard/internal/telemetry"
)

type Canonicalizer interface {
	RunTo(context.Context, uint64) (backfill.Result, error)
}
type HeadSource interface {
	BlockNumber(context.Context) (uint64, error)
}
type Sweeper interface {
	Sweep(context.Context) (backfill.Result, uint64, error)
}
type Subscriber interface {
	Subscribe(context.Context, func(), func(rpc.Hint)) error
}

type Config struct {
	PollInterval time.Duration
	SweepTimeout time.Duration
	BackoffMin   time.Duration
	BackoffMax   time.Duration
	LagTolerance uint64
	// Jitter is injectable for deterministic tests. nil adds up to 25% jitter.
	Jitter func(time.Duration) time.Duration
}

func (c Config) Validate() error {
	if c.PollInterval <= 0 || c.SweepTimeout <= 0 || c.BackoffMin <= 0 || c.BackoffMax < c.BackoffMin || c.BackoffMax > time.Minute || c.LagTolerance > 100000 {
		return errors.New("invalid live configuration")
	}
	return nil
}

type Status struct {
	State                 string
	Reason                string
	Ready                 bool
	WSConnected           bool
	RemoteHead            uint64
	Checkpoint            uint64
	Lag                   uint64
	Hints                 uint64
	DuplicateOrStaleHints uint64
	RemovedHints          uint64
	Reconnects            uint64
	ReconnectBackoff      time.Duration
	CatchupRanges         uint64
	PollingFallbacks      uint64
	TransportFailures     map[string]uint64
}

type Service struct {
	Canonical Canonicalizer
	Head      HeadSource
	Syncer    Sweeper    // optional pinned multi-provider sweep
	WS        Subscriber // nil means HTTP-only fallback
	Config    Config
	Logger    *slog.Logger

	mu       sync.Mutex
	status   Status
	wake     chan struct{}
	lastHint map[rpc.HintKind]rpc.Hint
}

func (s *Service) Status() Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	x := s.status
	x.TransportFailures = make(map[string]uint64, len(s.status.TransportFailures))
	for k, v := range s.status.TransportFailures {
		x.TransportFailures[k] = v
	}
	return x
}

// WritePrometheus exposes a fixed set of series and bounded failure reasons.
func (s *Service) WritePrometheus(w io.Writer) error {
	x := s.Status()
	connected, ready := 0, 0
	if x.WSConnected {
		connected = 1
	}
	if x.Ready {
		ready = 1
	}
	for _, metric := range []struct {
		name  string
		value any
	}{
		{"reorgguard_ws_connected", connected},
		{"reorgguard_live_ready", ready},
		{"reorgguard_ws_reconnects_total", x.Reconnects},
		{"reorgguard_ws_reconnect_backoff_seconds", x.ReconnectBackoff.Seconds()},
		{"reorgguard_ws_hints_total", x.Hints},
		{"reorgguard_ws_duplicate_or_stale_hints_total", x.DuplicateOrStaleHints},
		{"reorgguard_ws_removed_hints_total", x.RemovedHints},
		{"reorgguard_http_catchup_ranges_total", x.CatchupRanges},
		{"reorgguard_polling_fallback_total", x.PollingFallbacks},
		{"reorgguard_live_lag_blocks", x.Lag},
	} {
		if _, err := fmt.Fprintf(w, "%s %v\n", metric.name, metric.value); err != nil {
			return err
		}
	}
	for _, reason := range []string{"http", "ws_transport", "ws_malformed"} {
		if _, err := fmt.Fprintf(w, "reorgguard_live_transport_failures_total{reason=%q} %d\n", reason, x.TransportFailures[reason]); err != nil {
			return err
		}
	}
	return nil
}
func (s *Service) logger() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return slog.Default()
}
func (s *Service) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}
func (s *Service) hint(h rpc.Hint) {
	s.mu.Lock()
	s.status.Hints++
	if h.Removed {
		s.status.RemovedHints++
	}
	if last, ok := s.lastHint[h.Kind]; (ok && last == h) || h.Number < s.status.Checkpoint {
		s.status.DuplicateOrStaleHints++
	}
	s.lastHint[h.Kind] = h
	s.mu.Unlock()
	s.signal()
}
func (s *Service) setConnection(connected bool) {
	s.mu.Lock()
	s.status.WSConnected = connected
	s.mu.Unlock()
	if connected {
		s.signal()
	} // reconnect always starts with HTTP checkpoint sweep
}
func (s *Service) failure(kind string) {
	s.mu.Lock()
	s.status.TransportFailures[kind]++
	s.mu.Unlock()
}
func (s *Service) setUnsafe(reason string) {
	s.mu.Lock()
	s.status.State = "unsafe"
	s.status.Reason = reason
	s.status.Ready = false
	s.mu.Unlock()
}

// Run owns one WS/reconnect goroutine. This caller is the sole HTTP writer and
// polling owner; wake has capacity one regardless of notification volume.
func (s *Service) Run(ctx context.Context) error {
	if s.Syncer == nil && (s.Canonical == nil || s.Head == nil) {
		return errors.New("invalid live dependencies")
	}
	if err := s.Config.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	s.status = Status{State: "starting", TransportFailures: make(map[string]uint64)}
	s.lastHint = make(map[rpc.HintKind]rpc.Hint)
	s.wake = make(chan struct{}, 1)
	s.mu.Unlock()
	if err := s.sweep(ctx, false); err != nil && terminal(err) {
		s.setUnsafe(reason(err))
		return err
	}
	work, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	terminalWS := make(chan error, 1)
	if s.WS != nil {
		wg.Add(1)
		go func() { defer wg.Done(); s.reconnect(work, terminalWS) }()
	}
	defer func() { cancel(); wg.Wait() }()
	ticker := time.NewTicker(s.Config.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-terminalWS:
			s.setUnsafe("wrong_ws_chain")
			return err
		case <-ticker.C:
		case <-s.wake:
		}
		if err := s.sweep(ctx, true); err != nil && terminal(err) {
			s.setUnsafe(reason(err))
			return err
		}
	}
}

func (s *Service) sweep(ctx context.Context, afterStartup bool) error {
	ctx, span := telemetry.Start(ctx, "live.http_sweep")
	defer span.End()
	work, cancel := context.WithTimeout(ctx, s.Config.SweepTimeout)
	defer cancel()
	var target uint64
	var result backfill.Result
	var err error
	if s.Syncer != nil {
		result, target, err = s.Syncer.Sweep(work)
	} else {
		target, err = s.Head.BlockNumber(work)
		if err == nil {
			result, err = s.Canonical.RunTo(work, target)
			if err == nil {
				// Observe head again after potentially long catch-up so readiness can
				// report bounded lag rather than claiming the earlier target is current.
				latest, latestErr := s.Head.BlockNumber(work)
				if latestErr != nil {
					err = latestErr
				} else {
					target = latest
				}
			}
		}
	}
	if err == nil {
		s.mu.Lock()
		s.status.RemoteHead = target
		s.status.Checkpoint = result.Checkpoint.Number
		s.status.Lag = 0
		if target > result.Checkpoint.Number {
			s.status.Lag = target - result.Checkpoint.Number
		}
		if afterStartup && !s.status.WSConnected {
			s.status.PollingFallbacks++
		}
		if afterStartup {
			s.status.CatchupRanges += uint64(result.Ranges)
		}
		s.status.Ready = s.status.Lag <= s.Config.LagTolerance
		s.status.Reason = ""
		s.status.State = "healthy_caught_up"
		if s.status.Lag > 0 {
			s.status.State = "healthy_lagging"
		}
		if !s.status.WSConnected {
			s.status.State = "ws_degraded_http_healthy"
		}
		if !s.status.Ready {
			s.status.State = "lag_exceeded"
			s.status.Reason = "lag_exceeded"
		}
		s.mu.Unlock()
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if terminal(err) {
		s.setUnsafe(reason(err))
		return err
	}
	s.failure("http")
	s.mu.Lock()
	s.status.Ready = false
	s.status.State = "http_unavailable"
	s.status.Reason = "http_unavailable"
	s.mu.Unlock()
	s.logger().Warn("live HTTP canonical sweep failed", "reason", "http_unavailable")
	return err
}

func terminal(err error) bool {
	return errors.Is(err, backfill.ErrWrongChain) || errors.Is(err, multirpc.ErrIncompatible) || errors.Is(err, store.ErrUnsafe) || errors.Is(err, store.ErrConfigMismatch) || errors.Is(err, store.ErrCorruptCanonical) || errors.Is(err, model.ErrIdentity) || errors.Is(err, model.ErrParent) || errors.Is(err, model.ErrInvalidRange) || errors.Is(err, reorg.ErrDepth) || errors.Is(err, reorg.ErrAncestorMissing) || errors.Is(err, reorg.ErrParentChain)
}
func reason(err error) string {
	switch {
	case errors.Is(err, backfill.ErrWrongChain):
		return "wrong_http_chain"
	case errors.Is(err, reorg.ErrDepth):
		return "depth_exceeded"
	case errors.Is(err, reorg.ErrAncestorMissing):
		return "ancestor_missing"
	case errors.Is(err, model.ErrIdentity):
		return "identity_conflict"
	default:
		return "unsafe_canonicality"
	}
}

func (s *Service) reconnect(ctx context.Context, terminalWS chan<- error) {
	attempt := 0
	for ctx.Err() == nil {
		err := s.WS.Subscribe(ctx, func() { attempt = 0; s.setConnection(true) }, s.hint)
		s.setConnection(false)
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, rpc.ErrWSWrongChain) {
			select {
			case terminalWS <- err:
			case <-ctx.Done():
			}
			return
		}
		kind := "ws_transport"
		if errors.Is(err, rpc.ErrWSMalformed) {
			kind = "ws_malformed"
		}
		s.failure(kind)
		attempt++
		base := s.Config.BackoffMin
		for i := 1; i < attempt && base < s.Config.BackoffMax; i++ {
			if base > s.Config.BackoffMax/2 {
				base = s.Config.BackoffMax
				break
			}
			base *= 2
		}
		if base > s.Config.BackoffMax {
			base = s.Config.BackoffMax
		}
		jitter := s.Config.Jitter
		if jitter == nil {
			jitter = func(d time.Duration) time.Duration { return time.Duration(rand.Int63n(int64(d/4 + 1))) }
		}
		delay := base + jitter(base)
		if delay < base {
			delay = base
		}
		if delay > s.Config.BackoffMax {
			delay = s.Config.BackoffMax
		}
		s.mu.Lock()
		s.status.Reconnects++
		s.status.ReconnectBackoff = delay
		s.mu.Unlock()
		s.logger().Warn("WebSocket subscription unavailable", "reason", kind, "backoff_ms", delay.Milliseconds())
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
