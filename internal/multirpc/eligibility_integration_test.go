//go:build integration

package multirpc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"reorgguard/internal/model"
	"reorgguard/internal/reorg"
	"reorgguard/internal/store"
)

func TestCheckpointRelativeRejectionExpiresAfterBranchSwitch(t *testing.T) {
	db, _ := testStore(t)
	primary, fallback := newFixtureSource(), newFixtureSource()
	primary.setBranch(4, 0, 0)
	fallback.setBranch(4, 0, 0)
	pool := testPool(t, db, primary, fallback)
	sweep(t, pool)
	primary.setBranch(4, 3, 1)
	fallback.setBranch(4, 3, 1)
	sweep(t, pool)
	if checkpoint(t, db).Hash != testHash(1, 4) {
		t.Fatal("primary did not commit the replacement branch")
	}
	primary.failMethod("chain", errors.New("primary offline"), -1)
	fallback.setBranch(5, 3, 1)
	if _, _, err := pool.Sweep(context.Background()); err != nil {
		t.Fatalf("fallback matches the new durable checkpoint: %v", err)
	}
	if checkpoint(t, db).Hash != testHash(1, 5) {
		t.Fatal("compatible fallback did not continue")
	}
}

func TestFreshPoolReconcilesPersistedShallowFork(t *testing.T) {
	db, dsn := testStore(t)
	primary, fallback := newFixtureSource(), newFixtureSource()
	primary.setBranch(4, 0, 0)
	fallback.setBranch(4, 0, 0)
	sweep(t, testPool(t, db, primary, fallback))
	primary.setBranch(4, 3, 1)
	fallback.setBranch(4, 3, 1)
	reopened, err := store.Open(context.Background(), dsn, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, _, err = testPool(t, reopened, primary, fallback).Sweep(context.Background()); err != nil {
		t.Fatalf("restart prevented proven shallow reconciliation: %v", err)
	}
	if checkpoint(t, reopened).Hash != testHash(1, 4) {
		t.Fatal("restart did not commit B head")
	}
	reference, refDSN := testStore(t)
	clean := newFixtureSource()
	clean.setBranch(4, 3, 1)
	sweep(t, testPool(t, reference, clean))
	want, _, _ := canonicalChecksum(t, refDSN)
	got, _, _ := canonicalChecksum(t, dsn)
	if got != want {
		t.Fatalf("restarted reorg projection %s, reference %s", got, want)
	}
}

func TestReturningBranchRevalidatesEndpointAtEachCheckpoint(t *testing.T) {
	db, _ := testStore(t)
	primary, fallback := newFixtureSource(), newFixtureSource()
	primary.setBranch(4, 0, 0)
	fallback.setBranch(4, 0, 0)
	pool := testPool(t, db, primary, fallback)
	sweep(t, pool)
	primary.setBranch(4, 3, 1)
	fallback.setBranch(4, 3, 1)
	sweep(t, pool)
	primary.setBranch(4, 0, 0)
	fallback.setBranch(4, 0, 0)
	sweep(t, pool)
	primary.failMethod("chain", errors.New("offline"), -1)
	fallback.setBranch(5, 0, 0)
	if _, _, err := pool.Sweep(context.Background()); err != nil || checkpoint(t, db).Hash != testHash(0, 5) {
		t.Fatalf("A-B-A eligibility failed: %v checkpoint=%+v", err, checkpoint(t, db))
	}
}

type movingAboveFrontier struct {
	*fixtureSource
	moved bool
}

func (s *movingAboveFrontier) BlockByNumber(ctx context.Context, n uint64) (model.Block, error) {
	b, err := s.fixtureSource.BlockByNumber(ctx, n)
	if n == 3 && !s.moved {
		s.moved = true
		s.setBranch(4, 3, 1)
	}
	return b, err
}

func TestMovingUncommittedRangeRetriesWithoutUnsafeMarker(t *testing.T) {
	db, _ := testStore(t)
	source := newFixtureSource()
	source.setBranch(2, 0, 0)
	pool := testPool(t, db, source)
	sweep(t, pool)
	source.setBranch(4, 0, 0)
	pool.endpoints[0].source.(*observedSource).base = &movingAboveFrontier{fixtureSource: source}
	if _, _, err := pool.Sweep(context.Background()); err != nil {
		t.Fatalf("upstream moved only above durable height 2: %v", err)
	}
	ready, err := db.Readiness(context.Background(), testChain)
	if err != nil || ready.State == "unsafe" || checkpoint(t, db).Hash != testHash(1, 4) {
		t.Fatalf("movement poisoned state: readiness=%+v err=%v", ready, err)
	}
}

type cancelAtCandidateRecheck struct {
	*fixtureSource
	reads  int
	cancel context.CancelFunc
}

func (s *cancelAtCandidateRecheck) BlockByNumber(ctx context.Context, n uint64) (model.Block, error) {
	s.reads++
	if s.reads == 2 {
		s.cancel()
		return model.Block{}, ctx.Err()
	}
	return s.fixtureSource.BlockByNumber(ctx, n)
}

func TestCancelledRecheckLeavesRestartHealthy(t *testing.T) {
	db, dsn := testStore(t)
	source := newFixtureSource()
	source.setBranch(4, 0, 0)
	pool := testPool(t, db, source)
	sweep(t, pool)
	source.setBranch(4, 3, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.endpoints[0].engine.Source = &cancelAtCandidateRecheck{fixtureSource: source, cancel: cancel}
	if _, err := pool.endpoints[0].engine.Reconcile(ctx, 4); !errors.Is(err, context.Canceled) {
		t.Fatalf("lost cancellation: %v", err)
	}
	ready, _ := db.Readiness(context.Background(), testChain)
	if ready.State == "unsafe" || checkpoint(t, db).Hash != testHash(0, 4) {
		t.Fatalf("cancellation changed durable state: %+v", ready)
	}
	reopened, err := store.Open(context.Background(), dsn, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	ready, _ = reopened.Readiness(context.Background(), testChain)
	if ready.State == "unsafe" {
		t.Fatalf("restart inherited unsafe marker: %+v", ready)
	}
	sweep(t, testPool(t, reopened, source))
	if checkpoint(t, reopened).Hash != testHash(1, 4) {
		t.Fatal("retry did not reconcile B")
	}
}

func TestSingleSourceRejectsChangedCheckpointIdentity(t *testing.T) {
	for _, change := range []string{"parent", "timestamp", "logs"} {
		t.Run(change, func(t *testing.T) {
			db, _ := testStore(t)
			source := newFixtureSource()
			source.setBranch(3, 0, 0)
			pool := testPool(t, db, source)
			if _, err := pool.endpoints[0].runner.RunTo(context.Background(), 2); err != nil {
				t.Fatal(err)
			}
			source.mu.Lock()
			b := source.active[2]
			switch change {
			case "parent":
				b.ParentHash = common.Hash{31: 99}
				source.active[2] = b
			case "timestamp":
				b.Time = b.Time.Add(time.Second)
				source.active[2] = b
			case "logs":
				source.logs[b.Hash] = []model.Log{{BlockHash: b.Hash, BlockNumber: 2, TxHash: testHash(99, 2), Address: testFilter().Addresses[0]}}
			}
			source.mu.Unlock()
			if _, err := pool.endpoints[0].runner.RunTo(context.Background(), 3); err == nil {
				t.Fatal("changed immutable checkpoint accepted")
			}
			if checkpoint(t, db).Number != 2 {
				t.Fatal("checkpoint advanced after changed identity")
			}
		})
	}
}

func TestMixedOutOfFilterRangeCannotCommit(t *testing.T) {
	db, _ := testStore(t)
	source := newFixtureSource()
	source.setBranch(2, 0, 0)
	source.mu.Lock()
	valid := source.logs[testHash(0, 1)][0]
	invalid := valid
	invalid.Index = 1
	invalid.TxHash = testHash(101, 1)
	invalid.Address = common.Address{19: 99}
	source.logs[valid.BlockHash] = []model.Log{valid, invalid}
	source.mu.Unlock()
	pool := testPool(t, db, source)
	if _, _, err := pool.Sweep(context.Background()); err == nil {
		t.Fatal("mixed valid and out-of-filter log response committed")
	}
	if checkpoint(t, db).Number != 0 {
		t.Fatal("checkpoint advanced after invalid log response")
	}
	rows, err := db.CanonicalLogs(context.Background(), testChain, 1, 2)
	if err != nil || len(rows) != 0 {
		t.Fatalf("partial canonical log persistence: rows=%d err=%v", len(rows), err)
	}
}

func TestOutOfFilterReplacementLogCannotSwitchBranch(t *testing.T) {
	db, _ := testStore(t)
	source := newFixtureSource()
	source.setBranch(4, 0, 0)
	pool := testPool(t, db, source)
	sweep(t, pool)
	source.setBranch(4, 3, 1)
	source.mu.Lock()
	log := source.logs[testHash(1, 3)][0]
	log.Address = common.Address{19: 99}
	source.logs[log.BlockHash] = []model.Log{log}
	source.mu.Unlock()
	if _, _, err := pool.Sweep(context.Background()); err == nil {
		t.Fatal("out-of-filter replacement log switched canonical branch")
	}
	if checkpoint(t, db).Hash != testHash(0, 4) {
		t.Fatal("branch moved despite invalid block-hash log response")
	}
}

type delayedCheckpointRepository struct {
	Repository
	delay time.Duration
}

func (d delayedCheckpointRepository) Checkpoint(ctx context.Context, chainID uint64) (store.Checkpoint, error) {
	select {
	case <-ctx.Done():
		return store.Checkpoint{}, ctx.Err()
	case <-time.After(d.delay):
		return d.Repository.Checkpoint(ctx, chainID)
	}
}

func TestSweepTriesFallbackAfterPrimaryFailureDespiteCooldownExpiry(t *testing.T) {
	db, _ := testStore(t)
	primary, fallback := newFixtureSource(), newFixtureSource()
	primary.setBranch(8, 0, 0)
	fallback.setBranch(8, 0, 0)
	primary.rangeFail[3] = errors.New("range unavailable")
	pool := testPool(t, db, primary, fallback)
	pool.repo = delayedCheckpointRepository{Repository: db, delay: 3 * time.Millisecond}
	if _, _, err := pool.Sweep(context.Background()); err != nil {
		t.Fatalf("fallback never attempted: %v", err)
	}
	if checkpoint(t, db).Number != 8 || len(fallback.seenRanges()) < 2 {
		t.Fatal("fallback did not finish exact continuation")
	}
}

var _ reorg.Source = (*cancelAtCandidateRecheck)(nil)
