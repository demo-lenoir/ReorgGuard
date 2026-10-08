//go:build integration

package multirpc

import (
	"context"
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"reorgguard/internal/backfill"
	"reorgguard/internal/model"
	"reorgguard/internal/reorg"
	"reorgguard/internal/rpc"
)

// Only the height-bound response is corrupted. Headers and hash-bound logs
// continue to reproduce the original A3, including its completed log set.
type corruptCheckpointRange struct {
	*fixtureSource
	mutate                func(*model.Log)
	rangeReads, hashReads int
}

func (s *corruptCheckpointRange) Logs(ctx context.Context, from, to uint64, f rpc.Filter) ([]model.Log, error) {
	logs, err := s.fixtureSource.Logs(ctx, from, to, f)
	if err == nil && from == 3 && to == 3 {
		s.rangeReads++
		logs = append([]model.Log(nil), logs...)
		for i := range logs {
			logs[i].Topics = append([]common.Hash(nil), logs[i].Topics...)
			logs[i].Data = append([]byte(nil), logs[i].Data...)
			s.mutate(&logs[i])
		}
	}
	return logs, err
}

func (s *corruptCheckpointRange) LogsByBlockHash(ctx context.Context, h common.Hash, f rpc.Filter) ([]model.Log, error) {
	if h == testHash(0, 3) {
		s.hashReads++
	}
	return s.fixtureSource.LogsByBlockHash(ctx, h, f)
}

func testCheckpointRangeCorruption(t *testing.T, pooled bool) {
	t.Helper()
	for _, change := range []struct {
		name   string
		mutate func(*model.Log)
	}{
		{"data", func(l *model.Log) { l.Data = []byte{0xde, 0xad} }},
		{"topics", func(l *model.Log) { l.Topics[0] = testHash(91, 3) }},
		{"address", func(l *model.Log) { l.Address = common.Address{19: 2} }},
		{"transaction_hash", func(l *model.Log) { l.TxHash = testHash(101, 3) }},
		{"transaction_index", func(l *model.Log) { l.TxIndex++ }},
		{"log_index", func(l *model.Log) { l.Index++ }},
		{"block_number", func(l *model.Log) { l.BlockNumber++ }},
	} {
		t.Run(change.name, func(t *testing.T) {
			ctx := context.Background()
			db, dsn := testStore(t)
			source := newFixtureSource()
			source.setBranch(3, 0, 0)
			config := testPool(t, db, source).config
			// Both addresses are in-filter: the address mutation must be rejected
			// as immutable corruption, independently of filter enforcement.
			config.Filter.Addresses = append(config.Filter.Addresses, common.Address{19: 2})
			config.Runner.Filter = config.Filter
			config.Engine.Filter = config.Filter
			pool, err := New(db, []Endpoint{{ID: "primary", Source: source}}, config)
			if err != nil {
				t.Fatal(err)
			}
			sweep(t, pool)
			before := checkpoint(t, db)
			wantChecksum, wantBlocks, wantLogs := canonicalChecksum(t, dsn)
			stored, err := db.CanonicalLogs(ctx, testChain, 0, 3)
			if err != nil {
				t.Fatal(err)
			}
			wantLogIdentity := model.FingerprintLogs(stored)
			corrupt := &corruptCheckpointRange{fixtureSource: source, mutate: change.mutate}
			pool.endpoints[0].source.(*observedSource).base = corrupt
			wantError := model.ErrIdentity
			if pooled {
				// Sweep reports aggregate availability; endpoint health retains the
				// immutable incompatibility rather than a transient retry reason.
				wantError = ErrNoEligible
				_, _, err = pool.Sweep(ctx)
				health := pool.Status().Endpoints[0]
				if health.State != "incompatible" || health.Reason != "checkpoint_history_mismatch" {
					t.Errorf("immutable corruption classified as %s/%s", health.State, health.Reason)
				}
			} else {
				coordinator := &reorg.Coordinator{Backfill: pool.endpoints[0].runner, Engine: pool.endpoints[0].engine}
				_, err = coordinator.RunTo(ctx, 3)
			}
			if !errors.Is(err, wantError) || errors.Is(err, backfill.ErrCheckpointMoved) || errors.Is(err, reorg.ErrCandidateMoved) {
				t.Errorf("same-hash corruption must be identity failure, got %v", err)
			}
			if corrupt.rangeReads != 1 || corrupt.hashReads != 1 {
				t.Errorf("corruption was retried as movement: range=%d hash=%d", corrupt.rangeReads, corrupt.hashReads)
			}
			if got := checkpoint(t, db); got != before {
				t.Fatalf("checkpoint changed: %+v -> %+v", before, got)
			}
			gotChecksum, gotBlocks, gotLogs := canonicalChecksum(t, dsn)
			if gotChecksum != wantChecksum || gotBlocks != wantBlocks || gotLogs != wantLogs {
				t.Fatal("canonical projection changed")
			}
			rows, err := db.CanonicalLogs(ctx, testChain, 0, 3)
			if err != nil || model.FingerprintLogs(rows) != wantLogIdentity {
				t.Fatalf("immutable log fields changed: %v", err)
			}
		})
	}
}

func TestDirectCheckpointRejectsSameHashCorruptedRange(t *testing.T) {
	testCheckpointRangeCorruption(t, false)
}

func TestPoolCheckpointRejectsSameHashCorruptedRange(t *testing.T) {
	testCheckpointRangeCorruption(t, true)
}
