//go:build integration

package multirpc

import (
	"context"
	"testing"

	"reorgguard/internal/backfill"
	"reorgguard/internal/model"
	"reorgguard/internal/reorg"
	"reorgguard/internal/store"
)

// checkpointMovesAfterRead returns A3 for the first height lookup, then
// exposes B3 by height while keeping A3 available by its immutable hash.
type checkpointMovesAfterRead struct {
	*fixtureSource
	moved bool
}

func (s *checkpointMovesAfterRead) BlockByNumber(ctx context.Context, n uint64) (model.Block, error) {
	b, err := s.fixtureSource.BlockByNumber(ctx, n)
	if n == 3 && err == nil && !s.moved {
		s.moved = true
		s.setBranch(3, 3, 1)
	}
	return b, err
}

func checkpointMovementReference(t *testing.T) string {
	t.Helper()
	reference, dsn := testStore(t)
	source := newFixtureSource()
	source.setBranch(3, 3, 1)
	sweep(t, testPool(t, reference, source))
	want, _, _ := canonicalChecksum(t, dsn)
	return want
}

func assertCheckpointMovementResult(t *testing.T, db *store.Store, dsn, want string) {
	t.Helper()
	cp := checkpoint(t, db)
	if cp.Number != 3 || cp.Hash != testHash(1, 3) {
		t.Fatalf("reorg did not reach B3: %+v", cp)
	}
	ready, err := db.Readiness(context.Background(), testChain)
	if err != nil || ready.State == "unsafe" {
		t.Fatalf("legitimate movement marked unsafe: %+v, %v", ready, err)
	}
	got, blocks, logs := canonicalChecksum(t, dsn)
	if got != want || blocks != 4 || logs != 2 {
		t.Fatalf("canonical projection differs from clean B: got=%s want=%s blocks=%d logs=%d", got, want, blocks, logs)
	}
}

func TestPoolCheckpointMovesBetweenHeaderAndRangeLogs(t *testing.T) {
	db, dsn := testStore(t)
	source := newFixtureSource()
	source.setBranch(3, 0, 0)
	pool := testPool(t, db, source)
	sweep(t, pool)
	moving := &checkpointMovesAfterRead{fixtureSource: source}
	pool.endpoints[0].source.(*observedSource).base = moving
	want := checkpointMovementReference(t)
	if _, _, err := pool.Sweep(context.Background()); err != nil {
		t.Fatalf("checkpoint-time movement prevented bounded reconciliation: %v", err)
	}
	if !moving.moved {
		t.Fatal("fixture did not move between checkpoint reads")
	}
	assertCheckpointMovementResult(t, db, dsn, want)
}

// DirectCheckpointMovementFixture is shared with the external-package live
// regression, which imports live without creating a package import cycle.
func DirectCheckpointMovementFixture(t *testing.T) (*reorg.Coordinator, backfill.Source, *store.Store, string, string) {
	t.Helper()
	db, dsn := testStore(t)
	source := newFixtureSource()
	source.setBranch(3, 0, 0)
	pool := testPool(t, db, source)
	if _, err := pool.endpoints[0].runner.RunTo(context.Background(), 3); err != nil {
		t.Fatal(err)
	}
	moving := &checkpointMovesAfterRead{fixtureSource: source}
	pool.endpoints[0].runner.Source = moving
	pool.endpoints[0].engine.Source = moving
	want := checkpointMovementReference(t)
	return &reorg.Coordinator{Backfill: pool.endpoints[0].runner, Engine: pool.endpoints[0].engine}, moving, db, dsn, want
}

// CheckDirectMovementProjection uses independent SQL inspection rather than
// trusting a successful live service exit or its in-memory status.
func CheckDirectMovementProjection(t *testing.T, db *store.Store, dsn, want string) {
	t.Helper()
	assertCheckpointMovementResult(t, db, dsn, want)
}
