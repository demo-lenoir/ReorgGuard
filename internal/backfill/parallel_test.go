package backfill

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"reorgguard/internal/model"
	"reorgguard/internal/rpc"
)

func parallelRunner() *Runner {
	r := newRunner(8)
	r.Config.InitialRange = 2
	r.Config.MaxRange = 2
	r.Config.Workers = 4
	r.Config.MaxInFlight = 4
	r.Config.MaxAttempts = 1
	r.Parallel = &ParallelStats{}
	return r
}
func TestParallelReverseAndRandomCompletion(t *testing.T) {
	for _, order := range [][]uint64{{7, 5, 3, 1}, {5, 1, 7, 3}} {
		t.Run(fmt.Sprint(order), func(t *testing.T) {
			r := parallelRunner()
			arrived := make(chan uint64, 4)
			completed := make(chan uint64, 4)
			gates := map[uint64]chan struct{}{}
			for _, from := range []uint64{1, 3, 5, 7} {
				gates[from] = make(chan struct{})
			}
			r.Source.(*source).logs = func(ctx context.Context, from, to uint64) ([]model.Log, error) {
				arrived <- from
				select {
				case <-gates[from]:
					completed <- from
					return nil, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			done := make(chan error, 1)
			go func() { _, err := r.Run(context.Background()); done <- err }()
			for i := 0; i < 4; i++ {
				select {
				case <-arrived:
				case <-time.After(time.Second):
					t.Fatal("workers did not start")
				}
			}
			for _, from := range order {
				close(gates[from])
				select {
				case got := <-completed:
					if got != from {
						t.Fatalf("RPC completion %d, want %d", got, from)
					}
				case <-time.After(time.Second):
					t.Fatal("range did not complete")
				}
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("ordered writer blocked")
			}
			want := [][2]uint64{{1, 2}, {3, 4}, {5, 6}, {7, 8}}
			if got := r.Store.(*repository).calls; !reflect.DeepEqual(got, want) {
				t.Fatalf("commit order %v", got)
			}
			stats := r.Parallel.Snapshot()
			if stats.MaxActive > 4 || stats.MaxInFlight > 4 || stats.MaxPending > 4 || stats.Active != 0 || stats.InFlight != 0 || stats.Pending != 0 {
				t.Fatalf("bounds %+v", stats)
			}
		})
	}
}
func TestParallelSlowFirstCannotAdvanceCheckpoint(t *testing.T) {
	r := parallelRunner()
	release := make(chan struct{})
	later := make(chan struct{}, 3)
	r.Source.(*source).logs = func(ctx context.Context, from, to uint64) ([]model.Log, error) {
		if from == 1 {
			select {
			case <-release:
				return nil, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		later <- struct{}{}
		return nil, nil
	}
	done := make(chan error, 1)
	go func() { _, err := r.Run(context.Background()); done <- err }()
	for i := 0; i < 3; i++ {
		select {
		case <-later:
		case <-time.After(time.Second):
			t.Fatal("later range did not finish")
		}
	}
	if r.Store.(*repository).cp.Number != 0 {
		t.Fatal("later range advanced checkpoint")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if r.Store.(*repository).cp.Number != 8 {
		t.Fatal("missing final checkpoint")
	}
}
func TestParallelMiddleFailureNoGap(t *testing.T) {
	r := parallelRunner()
	r.Source.(*source).logs = func(_ context.Context, from, to uint64) ([]model.Log, error) {
		if from == 3 {
			return nil, errors.New("provider failed")
		}
		return nil, nil
	}
	_, err := r.Run(context.Background())
	if !errors.Is(err, ErrAttempts) {
		t.Fatalf("middle failure %v", err)
	}
	if cp := r.Store.(*repository).cp.Number; cp != 2 {
		t.Fatalf("checkpoint skipped failed range: %d", cp)
	}
	stats := r.Parallel.Snapshot()
	if stats.Active != 0 || stats.InFlight != 0 || stats.Pending != 0 {
		t.Fatalf("worker leak %+v", stats)
	}
}

type failingAppend struct {
	*repository
	later    <-chan struct{}
	required int
}

func (s *failingAppend) Append(ctx context.Context, _ uint64, _ []model.BlockData) error {
	for i := 0; i < s.required; i++ {
		select {
		case <-s.later:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return errors.New("DB commit failed")
}
func TestParallelDBFailureDiscardsLaterResults(t *testing.T) {
	r := parallelRunner()
	later := make(chan struct{}, 3)
	r.Source.(*source).logs = func(ctx context.Context, from, _ uint64) ([]model.Log, error) {
		if from != 1 {
			later <- struct{}{}
		} else {
			// Hold the first range until all three later results really occupy
			// the bounded result buffer. Signals from Logs alone precede the
			// subsequent header/validation work and gave a scheduler race.
			for r.Parallel.Snapshot().Pending < 3 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				runtime.Gosched()
			}
		}
		return nil, nil
	}
	repo := &failingAppend{repository: &repository{}, later: later, required: 3}
	r.Store = repo
	_, err := r.Run(context.Background())
	if err == nil {
		t.Fatal("DB failure swallowed")
	}
	if repo.cp.Number != 0 || len(repo.calls) != 0 {
		t.Fatal("checkpoint moved on DB failure")
	}
	stats := r.Parallel.Snapshot()
	if stats.MaxPending < 3 || stats.Active != 0 || stats.InFlight != 0 || stats.Pending != 0 {
		t.Fatalf("worker leak %+v", stats)
	}
}
func TestParallelCancellationStopsWorkers(t *testing.T) {
	r := parallelRunner()
	arrived := make(chan struct{}, 4)
	r.Source.(*source).logs = func(ctx context.Context, _, _ uint64) ([]model.Log, error) {
		arrived <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := r.Run(ctx); done <- err }()
	for i := 0; i < 4; i++ {
		select {
		case <-arrived:
		case <-time.After(time.Second):
			t.Fatal("workers did not start")
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("workers blocked on shutdown")
	}
	stats := r.Parallel.Snapshot()
	if stats.Active != 0 || stats.InFlight != 0 || stats.Pending != 0 {
		t.Fatalf("worker leak %+v", stats)
	}
}
func TestParallelRangeShrinkAndSerialChecksum(t *testing.T) {
	r := parallelRunner()
	r.Config.InitialRange = 4
	r.Config.MaxRange = 4
	r.Config.MaxAttempts = 4
	var mu sync.Mutex
	requests := [][2]uint64{}
	r.Source.(*source).logs = func(_ context.Context, from, to uint64) ([]model.Log, error) {
		mu.Lock()
		requests = append(requests, [2]uint64{from, to})
		mu.Unlock()
		if to-from+1 > 2 {
			return nil, &rpc.RPCError{Code: -32005}
		}
		return nil, nil
	}
	res, err := r.Run(context.Background())
	if err != nil || res.Checkpoint.Number != 8 {
		t.Fatalf("parallel shrink %+v %v", res, err)
	}
	mu.Lock()
	sawLarge, sawRetry := false, false
	for _, v := range requests {
		if v == ([2]uint64{1, 4}) {
			sawLarge = true
		}
		if v == ([2]uint64{1, 2}) {
			sawRetry = true
		}
	}
	mu.Unlock()
	if !sawLarge || !sawRetry {
		t.Fatalf("same-start shrink missing: %v", requests)
	}
	serial := newRunner(8)
	serial.Config.InitialRange = 2
	serial.Config.MaxRange = 2
	if _, err := serial.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(r.Store.(*repository).blocks, serial.Store.(*repository).blocks) {
		t.Fatal("parallel canonical projection differs from serial")
	}
}
