package live

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"reorgguard/internal/backfill"
	"reorgguard/internal/model"
	"reorgguard/internal/rpc"
	"reorgguard/internal/store"
)

type rejectedCheckpointCanonical struct{ calls atomic.Int32 }

func (c *rejectedCheckpointCanonical) RunTo(context.Context, uint64) (backfill.Result, error) {
	c.calls.Add(1)
	return backfill.Result{}, model.ErrIdentity
}

func TestLiveCheckpointIdentityFailureCannotAdvance(t *testing.T) {
	head := &fixtureHead{}
	head.value.Store(3)
	canonical := &rejectedCheckpointCanonical{}
	s := &Service{Canonical: canonical, Head: head, Config: Config{
		PollInterval: time.Millisecond, SweepTimeout: time.Second,
		BackoffMin: time.Millisecond, BackoffMax: time.Second, LagTolerance: 2,
	}}
	err := s.Run(context.Background())
	if !errors.Is(err, model.ErrIdentity) || canonical.calls.Load() != 1 {
		t.Fatalf("live continued after immutable checkpoint conflict: %v", err)
	}
	status := s.Status()
	if status.Ready || status.State != "unsafe" || status.Checkpoint != 0 {
		t.Fatalf("live readiness or progress after checkpoint conflict: %+v", status)
	}
}

type fixtureHead struct {
	value atomic.Uint64
	fail  atomic.Bool
}

func (h *fixtureHead) BlockNumber(context.Context) (uint64, error) {
	if h.fail.Load() {
		return 0, errors.New("HTTP offline")
	}
	return h.value.Load(), nil
}

type fixtureCanonical struct {
	calls      chan uint64
	fail       atomic.Bool
	mu         sync.Mutex
	checkpoint uint64
}

func (c *fixtureCanonical) RunTo(ctx context.Context, target uint64) (backfill.Result, error) {
	if c.fail.Load() {
		return backfill.Result{}, errors.New("HTTP canonical fetch offline")
	}
	c.mu.Lock()
	prior := c.checkpoint
	if target > prior {
		c.checkpoint = target
	}
	cp := c.checkpoint
	c.mu.Unlock()
	select {
	case c.calls <- target:
	case <-ctx.Done():
		return backfill.Result{}, ctx.Err()
	}
	ranges := 0
	if target > prior {
		ranges = 1
	}
	return backfill.Result{Checkpoint: store.Checkpoint{Number: cp}, Ranges: ranges}, nil
}

type event struct {
	hint rpc.Hint
	err  error
}
type session struct {
	events chan event
	allow  chan struct{}
}

func (x *session) open()           { close(x.allow) }
func (x *session) hint(h rpc.Hint) { x.events <- event{hint: h} }
func (x *session) drop()           { x.events <- event{err: rpc.ErrWSTransport} }

type fixtureWS struct{ sessions chan *session }

func (w *fixtureWS) Subscribe(ctx context.Context, connected func(), hint func(rpc.Hint)) error {
	x := &session{events: make(chan event, 11000), allow: make(chan struct{})}
	select {
	case w.sessions <- x:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-x.allow:
	case <-ctx.Done():
		return ctx.Err()
	}
	connected()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e := <-x.events:
			if e.err != nil {
				return e.err
			}
			hint(e.hint)
		}
	}
}
func setup(t *testing.T, poll time.Duration, withWS bool) (*Service, *fixtureHead, *fixtureCanonical, *fixtureWS, context.CancelFunc, <-chan error) {
	t.Helper()
	h := &fixtureHead{}
	c := &fixtureCanonical{calls: make(chan uint64, 200)}
	w := &fixtureWS{sessions: make(chan *session, 100)}
	s := &Service{Canonical: c, Head: h, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Config: Config{PollInterval: poll, SweepTimeout: time.Second, BackoffMin: time.Millisecond, BackoffMax: 10 * time.Millisecond, LagTolerance: 2, Jitter: func(time.Duration) time.Duration { return 0 }}}
	if withWS {
		s.WS = w
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() { done <- s.Run(ctx); close(finished) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Error("service leaked")
		}
	})
	return s, h, c, w, cancel, done
}
func call(t *testing.T, c *fixtureCanonical) uint64 {
	t.Helper()
	select {
	case n := <-c.calls:
		return n
	case <-time.After(time.Second):
		t.Fatal("missing HTTP sweep")
		return 0
	}
}
func connection(t *testing.T, w *fixtureWS) *session {
	t.Helper()
	select {
	case x := <-w.sessions:
		return x
	case <-time.After(time.Second):
		t.Fatal("missing WS connection")
		return nil
	}
}
func hint(n uint64, id byte) rpc.Hint {
	var h common.Hash
	h[31] = id
	return rpc.Hint{Kind: rpc.HeadHint, Number: n, Hash: h}
}
func statusEventually(t *testing.T, s *Service, pred func(Status) bool) Status {
	t.Helper()
	deadline := time.After(time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		x := s.Status()
		if pred(x) {
			return x
		}
		select {
		case <-deadline:
			t.Fatalf("status did not converge: %+v", x)
			return x
		case <-tick.C:
		}
	}
}
func TestLiveHeadsDuplicateOutOfOrderBurst(t *testing.T) {
	s, h, c, w, _, _ := setup(t, time.Hour, true)
	if call(t, c) != 0 {
		t.Fatal("initial target")
	}
	x := connection(t, w)
	x.open()
	call(t, c) // reconnect catch-up
	h.value.Store(3)
	x.hint(hint(3, 3))
	if call(t, c) != 3 {
		t.Fatal("head did not trigger HTTP")
	}
	x.hint(hint(3, 3))
	x.hint(hint(2, 2))
	logHint := rpc.Hint{Kind: rpc.LogHint, Number: 3, Hash: hint(3, 3).Hash}
	x.hint(logHint)
	x.hint(logHint)
	for i := 0; i < 10000; i++ {
		x.hint(hint(3, 3))
	}
	statusEventually(t, s, func(z Status) bool { return z.Hints >= 10005 && z.DuplicateOrStaleHints >= 10001 })
	if cap(s.wake) != 1 || len(s.wake) > 1 {
		t.Fatal("unbounded wake queue")
	}
	if s.Status().Checkpoint != 3 {
		t.Fatalf("checkpoint %+v", s.Status())
	}
}
func TestLiveMalformedSubscriptionReconnects(t *testing.T) {
	s, _, c, w, _, _ := setup(t, time.Hour, true)
	call(t, c)
	x := connection(t, w)
	x.open()
	call(t, c)
	x.events <- event{err: rpc.ErrWSMalformed}
	y := connection(t, w)
	y.open()
	call(t, c)
	statusEventually(t, s, func(z Status) bool { return z.Ready && z.Reconnects >= 1 && z.TransportFailures["ws_malformed"] == 1 })
}

type wrongChainWS struct{}

func (wrongChainWS) Subscribe(context.Context, func(), func(rpc.Hint)) error {
	return rpc.ErrWSWrongChain
}
func TestLiveWrongWSChainFailsClosed(t *testing.T) {
	h := &fixtureHead{}
	c := &fixtureCanonical{calls: make(chan uint64, 2)}
	s := &Service{Canonical: c, Head: h, WS: wrongChainWS{}, Config: Config{PollInterval: time.Hour, SweepTimeout: time.Second, BackoffMin: time.Millisecond, BackoffMax: time.Second}}
	err := s.Run(context.Background())
	if !errors.Is(err, rpc.ErrWSWrongChain) || s.Status().Ready || s.Status().State != "unsafe" {
		t.Fatalf("wrong WS chain accepted: err=%v status=%+v", err, s.Status())
	}
}
func TestLiveReconnectGapRecoveryAndRemoved(t *testing.T) {
	s, h, c, w, _, _ := setup(t, time.Hour, true)
	call(t, c)
	x := connection(t, w)
	x.open()
	call(t, c)
	h.value.Store(2)
	x.hint(hint(2, 2))
	if call(t, c) != 2 {
		t.Fatal("A not indexed")
	}
	statusEventually(t, s, func(z Status) bool { return z.Checkpoint == 2 })
	x.drop()
	y := connection(t, w)
	h.value.Store(7) // blocks advanced while disconnected
	y.open()
	if call(t, c) != 7 {
		t.Fatal("reconnect missed HTTP gap")
	}
	y.hint(rpc.Hint{Kind: rpc.LogHint, Number: 2, Removed: true})
	if call(t, c) != 7 {
		t.Fatal("removed hint bypassed HTTP")
	}
	statusEventually(t, s, func(z Status) bool { return z.RemovedHints == 1 && z.Reconnects >= 1 && z.Checkpoint == 7 })
	y.drop()
	z := connection(t, w)
	h.value.Store(9)
	z.open()
	if call(t, c) != 9 {
		t.Fatal("second reconnect gap")
	}
	z.hint(hint(7, 7))
	if call(t, c) != 9 {
		t.Fatal("stale hint changed target")
	}
}
func TestLiveHTTPFailureNeverAdvancesFromWS(t *testing.T) {
	s, h, c, w, _, _ := setup(t, 5*time.Millisecond, true)
	call(t, c)
	x := connection(t, w)
	x.open()
	call(t, c)
	h.value.Store(5)
	h.fail.Store(true)
	x.hint(hint(5, 5))
	statusEventually(t, s, func(z Status) bool { return z.State == "http_unavailable" })
	if s.Status().Checkpoint != 0 {
		t.Fatal("WS hint advanced checkpoint")
	}
	h.fail.Store(false)
	if call(t, c) != 5 {
		t.Fatal("poll did not recover")
	}
	statusEventually(t, s, func(z Status) bool { return z.Ready && z.Checkpoint == 5 })
}
func TestLivePollingFallback(t *testing.T) {
	s, h, c, _, _, _ := setup(t, 5*time.Millisecond, false)
	call(t, c)
	h.value.Store(4)
	if call(t, c) != 4 {
		t.Fatal("poll missed head")
	}
	statusEventually(t, s, func(z Status) bool { return z.State == "ws_degraded_http_healthy" && z.PollingFallbacks > 0 })
	var out bytes.Buffer
	if err := s.WritePrometheus(&out); err != nil || !bytes.Contains(out.Bytes(), []byte("reorgguard_ws_connected 0")) {
		t.Fatalf("metrics %v: %s", err, out.String())
	}
}
func TestLiveBothTransportsUnavailableAndRecovery(t *testing.T) {
	s, h, c, w, _, _ := setup(t, 5*time.Millisecond, true)
	call(t, c)
	x := connection(t, w)
	x.open()
	call(t, c)
	x.drop()
	h.fail.Store(true)
	statusEventually(t, s, func(z Status) bool { return z.State == "http_unavailable" && !z.Ready && !z.WSConnected })
	h.value.Store(6)
	h.fail.Store(false)
	y := connection(t, w)
	y.open()
	if call(t, c) != 6 {
		t.Fatal("HTTP recovery did not catch up")
	}
	statusEventually(t, s, func(z Status) bool { return z.Checkpoint == 6 && z.Ready })
}
func TestLiveGracefulStopAndNoGoroutineExplosion(t *testing.T) {
	baseline := runtime.NumGoroutine()
	for i := 0; i < 8; i++ {
		_, _, c, w, cancel, done := setup(t, time.Hour, true)
		call(t, c)
		x := connection(t, w)
		x.open()
		call(t, c)
		for j := 0; j < 10000; j++ {
			x.hint(hint(0, 1))
		}
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("shutdown blocked")
		}
	}
	if got := runtime.NumGoroutine(); got > baseline+20 {
		t.Fatalf("possible goroutine leak: before=%d after=%d", baseline, got)
	}
}
