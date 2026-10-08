//go:build integration

package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"reorgguard/internal/model"
)

const testChain = uint64(31337)

func forkBlock(n uint64, hashByte byte, parent common.Hash) model.Block {
	return model.Block{Number: n, Hash: testHash(hashByte), ParentHash: parent, Time: time.Unix(int64(n+1), 0).UTC()}
}
func withLogs(t *testing.T, ancestor common.Hash, blocks []model.Block, logged ...uint64) []model.BlockData {
	t.Helper()
	wantLog := map[uint64]bool{}
	for _, n := range logged {
		wantLog[n] = true
	}
	var logs []model.Log
	for _, b := range blocks {
		if wantLog[b.Number] {
			logs = append(logs, model.Log{BlockHash: b.Hash, BlockNumber: b.Number, TxHash: testHash(byte(180 + b.Number)), Index: 0, Address: common.Address{19: 1}, Topics: []common.Hash{testHash(91)}, Data: []byte{b.Hash[31]}})
		}
	}
	items, err := model.BuildRange(blocks[0].Number, blocks[len(blocks)-1].Number, ancestor, blocks, logs)
	if err != nil {
		t.Fatal(err)
	}
	return items
}
func appendA(t *testing.T, s *Store, end uint64, logged ...uint64) []model.BlockData {
	t.Helper()
	blocks := make([]model.Block, 0, end)
	for n := uint64(1); n <= end; n++ {
		blocks = append(blocks, testBlock(n))
	}
	items := withLogs(t, testHash(1), blocks, logged...)
	if _, err := s.Init(context.Background(), testDataset()); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(context.Background(), testChain, items); err != nil {
		t.Fatal(err)
	}
	return items
}
func replacement(t *testing.T, ancestor model.Block, end uint64, logged ...uint64) []model.BlockData {
	t.Helper()
	blocks := make([]model.Block, 0, end-ancestor.Number)
	parent := ancestor.Hash
	for n := ancestor.Number + 1; n <= end; n++ {
		b := forkBlock(n, byte(100+n), parent)
		blocks = append(blocks, b)
		parent = b.Hash
	}
	return withLogs(t, ancestor.Hash, blocks, logged...)
}
func assertState(t *testing.T, s *Store, expected ...model.Block) {
	t.Helper()
	ctx := context.Background()
	for _, b := range expected {
		got, err := s.CanonicalBlockByNumber(ctx, testChain, b.Number)
		if err != nil || got.Hash != b.Hash {
			t.Fatalf("height %d: %s %v, want %s", b.Number, got.Hash, err, b.Hash)
		}
	}
	last := expected[len(expected)-1]
	cp, err := s.Checkpoint(ctx, testChain)
	if err != nil || cp.Number != last.Number || cp.Hash != last.Hash {
		t.Fatalf("checkpoint %+v %v, want %d/%s", cp, err, last.Number, last.Hash)
	}
}

func TestReconcileDepthsAndMixedLogs(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		oldEnd, ancestor, newEnd uint64
		loggedA, loggedB         []uint64
	}{
		{"depth1", 3, 2, 3, []uint64{2, 3}, []uint64{3}},
		{"depth2", 4, 2, 4, []uint64{1, 3}, []uint64{4}},
		{"depth5", 7, 2, 7, []uint64{3, 5, 7}, []uint64{4, 6}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := testStore(t)
			a := appendA(t, s, tc.oldEnd, tc.loggedA...)
			b := replacement(t, a[tc.ancestor-1].Block, tc.newEnd, tc.loggedB...)
			res, err := s.Reconcile(context.Background(), testChain, Checkpoint{tc.oldEnd, a[len(a)-1].Block.Hash}, Checkpoint{tc.ancestor, a[tc.ancestor-1].Block.Hash}, b, tc.oldEnd-tc.ancestor)
			if err != nil {
				t.Fatal(err)
			}
			if res.Depth != tc.oldEnd-tc.ancestor || res.OrphanedBlocks != res.Depth || res.NewBlocks != uint64(len(b)) || res.RecanonicalizedBlocks != 0 {
				t.Fatalf("result %+v", res)
			}
			wantOrphans := uint64(0)
			for _, n := range tc.loggedA {
				if n > tc.ancestor {
					wantOrphans++
				}
			}
			if res.OrphanedLogs != wantOrphans {
				t.Fatalf("orphaned logs %+v", res)
			}
			for _, item := range b {
				got, lookupErr := s.CanonicalBlockByNumber(context.Background(), testChain, item.Block.Number)
				if lookupErr != nil || got.Hash != item.Block.Hash {
					t.Fatalf("canonical block %d: %v", item.Block.Number, lookupErr)
				}
			}
			assertState(t, s, b[len(b)-1].Block)
			logs, err := s.CanonicalLogs(context.Background(), testChain, 1, tc.newEnd)
			if err != nil {
				t.Fatal(err)
			}
			wantCanonicalLogs := 0
			for _, n := range tc.loggedA {
				if n <= tc.ancestor {
					wantCanonicalLogs++
				}
			}
			wantCanonicalLogs += len(tc.loggedB)
			if len(logs) != wantCanonicalLogs {
				t.Fatalf("canonical logs %d, want %d", len(logs), wantCanonicalLogs)
			}
		})
	}
}

func TestReconcileABATransactionAndRestart(t *testing.T) {
	s, cfg := testStore(t)
	ctx := context.Background()
	a := appendA(t, s, 4, 1, 3, 4)
	b := replacement(t, a[1].Block, 4, 3)
	oldA := Checkpoint{4, a[3].Block.Hash}
	anchor := Checkpoint{2, a[1].Block.Hash}
	toB, err := s.Reconcile(ctx, testChain, oldA, anchor, b, 2)
	if err != nil || toB.NewBlocks != 2 || toB.OrphanedLogs != 2 {
		t.Fatalf("A to B %+v %v", toB, err)
	}
	// Reopening must retain orphaned A identities and B's durable checkpoint.
	reopened, err := OpenConfig(ctx, cfg, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if cp, err := reopened.Init(ctx, testDataset()); err != nil || cp.Hash != b[1].Block.Hash {
		t.Fatalf("reopen %+v %v", cp, err)
	}
	toA, err := reopened.Reconcile(ctx, testChain, toB.NewHead, anchor, a[2:], 2)
	if err != nil || toA.RecanonicalizedBlocks != 2 || toA.NewBlocks != 0 {
		t.Fatalf("B to A %+v %v", toA, err)
	}
	for _, item := range a {
		got, lookupErr := reopened.CanonicalBlockByNumber(ctx, testChain, item.Block.Number)
		if lookupErr != nil || got.Hash != item.Block.Hash {
			t.Fatalf("A block %d: %v", item.Block.Number, lookupErr)
		}
	}
	assertState(t, reopened, a[len(a)-1].Block)
	for _, item := range b {
		stored, err := reopened.BlockByHash(ctx, testChain, item.Block.Hash)
		if err != nil || stored.Canonical {
			t.Fatalf("orphan B %+v %v", stored, err)
		}
	}
	logs, err := reopened.CanonicalLogs(ctx, testChain, 1, 4)
	if err != nil || len(logs) != 3 {
		t.Fatalf("A logs %d %v", len(logs), err)
	}
	var total int64
	if err = reopened.db.QueryRow(ctx, "SELECT count(*) FROM blocks WHERE chain_id=31337").Scan(&total); err != nil || total != 7 {
		t.Fatalf("audit blocks %d %v", total, err)
	}
	// Re-delivery after reconciliation remains exact and cannot duplicate rows.
	if err = reopened.Append(ctx, testChain, a[2:]); err != nil {
		t.Fatalf("replay A: %v", err)
	}
	if err = reopened.db.QueryRow(ctx, "SELECT count(*) FROM blocks WHERE chain_id=31337").Scan(&total); err != nil || total != 7 {
		t.Fatalf("duplicate blocks %d %v", total, err)
	}
	// A second B delivery must reactivate its old rows and logs as well.
	toBAgain, err := reopened.Reconcile(ctx, testChain, toA.NewHead, anchor, b, 2)
	if err != nil || toBAgain.RecanonicalizedBlocks != 2 || toBAgain.NewBlocks != 0 {
		t.Fatalf("duplicate replacement %+v %v", toBAgain, err)
	}
	assertState(t, reopened, b[len(b)-1].Block)
	if err = reopened.db.QueryRow(ctx, "SELECT count(*) FROM blocks WHERE chain_id=31337").Scan(&total); err != nil || total != 7 {
		t.Fatalf("duplicate audit rows %d %v", total, err)
	}
	if err = reopened.db.QueryRow(ctx, "SELECT count(*) FROM logs WHERE chain_id=31337").Scan(&total); err != nil || total != 4 {
		t.Fatalf("duplicate audit logs %d %v", total, err)
	}
}

func TestReconcileRejectsIdentityAndInvalidBranches(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func([]model.BlockData)
	}{
		{"changed block", func(items []model.BlockData) { items[0].Block.Time = items[0].Block.Time.Add(time.Second) }},
		{"changed log", func(items []model.BlockData) {
			items[0].Logs[0].Data = []byte{99}
			items[0].LogSetHash = model.FingerprintLogs(items[0].Logs)
		}},
		{"changed log set", func(items []model.BlockData) { items[0].Logs = nil; items[0].LogSetHash = model.FingerprintLogs(nil) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := testStore(t)
			a := appendA(t, s, 3, 3)
			b := replacement(t, a[1].Block, 3, 3)
			anchor := Checkpoint{2, a[1].Block.Hash}
			oldA := Checkpoint{3, a[2].Block.Hash}
			res, err := s.Reconcile(context.Background(), testChain, oldA, anchor, b, 1)
			if err != nil {
				t.Fatal(err)
			}
			known := append([]model.BlockData(nil), a[2:]...)
			known[0].Logs = append([]model.Log(nil), known[0].Logs...)
			tc.mutate(known)
			_, err = s.Reconcile(context.Background(), testChain, res.NewHead, anchor, known, 1)
			if !errors.Is(err, model.ErrIdentity) {
				t.Fatalf("identity error: %v", err)
			}
			assertState(t, s, b[0].Block)
		})
	}
	// Plausible heights with invalid parent hashes must fail before any switch.
	s, _ := testStore(t)
	a := appendA(t, s, 4)
	b := replacement(t, a[1].Block, 4)
	b[1].Block.ParentHash = testHash(240)
	_, err := s.Reconcile(context.Background(), testChain, Checkpoint{4, a[3].Block.Hash}, Checkpoint{2, a[1].Block.Hash}, b, 2)
	if !errors.Is(err, model.ErrParent) {
		t.Fatalf("corrupt parent accepted: %v", err)
	}
	assertState(t, s, a[3].Block)
}

func TestReconcileDepthAndMissingAncestor(t *testing.T) {
	s, _ := testStore(t)
	a := appendA(t, s, 4)
	b := replacement(t, a[1].Block, 4)
	old := Checkpoint{4, a[3].Block.Hash}
	ancestor := Checkpoint{2, a[1].Block.Hash}
	if _, err := s.Reconcile(context.Background(), testChain, old, ancestor, b, 1); !errors.Is(err, ErrDepth) {
		t.Fatalf("too deep: %v", err)
	}
	if _, err := s.Reconcile(context.Background(), testChain, old, Checkpoint{2, testHash(250)}, b, 2); !errors.Is(err, model.ErrParent) {
		t.Fatalf("wrong ancestor: %v", err)
	}
	if _, err := s.Reconcile(context.Background(), testChain, old, ancestor, b, 2); err != nil {
		t.Fatalf("exact depth: %v", err)
	}
	assertState(t, s, b[1].Block)
}

func TestReconcileRollbackAndRestart(t *testing.T) {
	for _, phase := range []string{"before commit", "during switch", "canceled lock"} {
		t.Run(phase, func(t *testing.T) {
			s, cfg := testStore(t)
			a := appendA(t, s, 4, 3, 4)
			b := replacement(t, a[1].Block, 4, 3)
			ctx := context.Background()
			old := Checkpoint{4, a[3].Block.Hash}
			anchor := Checkpoint{2, a[1].Block.Hash}
			var blocker pgx.Tx
			switch phase {
			case "before commit":
				s.beforeCommit = func(context.Context, pgx.Tx) error { return errors.New("injected precommit failure") }
			case "during switch":
				_, err := s.db.Exec(ctx, `CREATE FUNCTION reject_replacement() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.hash = decode('0000000000000000000000000000000000000000000000000000000000000067','hex') THEN RAISE EXCEPTION 'injected replacement failure'; END IF; RETURN NEW; END $$`)
				if err != nil {
					t.Fatal(err)
				}
				_, err = s.db.Exec(ctx, `CREATE TRIGGER reject_replacement BEFORE INSERT ON blocks FOR EACH ROW EXECUTE FUNCTION reject_replacement()`)
				if err != nil {
					t.Fatal(err)
				}
			case "canceled lock":
				var err error
				blocker, err = s.db.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer blocker.Rollback(ctx)
				if _, err = blocker.Exec(ctx, "UPDATE sync_state SET updated_at=now() WHERE chain_id=31337"); err != nil {
					t.Fatal(err)
				}
			}
			callCtx := ctx
			if phase == "canceled lock" {
				var cancel context.CancelFunc
				callCtx, cancel = context.WithTimeout(ctx, 25*time.Millisecond)
				defer cancel()
			}
			_, err := s.Reconcile(callCtx, testChain, old, anchor, b, 2)
			if err == nil {
				t.Fatal("expected reconciliation failure")
			}
			if phase == "canceled lock" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("cancel: %v", err)
			}
			if blocker != nil {
				if unlockErr := blocker.Rollback(ctx); unlockErr != nil {
					t.Fatal(unlockErr)
				}
			}
			assertState(t, s, a[3].Block)
			for _, item := range b {
				if _, err = s.BlockByHash(ctx, testChain, item.Block.Hash); !errors.Is(err, ErrBlockNotFound) {
					t.Fatalf("partial replacement block: %v", err)
				}
			}
			reopened, err := OpenConfig(ctx, cfg, 2*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			cp, err := reopened.Init(ctx, testDataset())
			if err != nil || cp.Hash != old.Hash {
				t.Fatalf("restart cp %+v %v", cp, err)
			}
		})
	}
}

func TestCorruptCheckpointFailsClosed(t *testing.T) {
	s, _ := testStore(t)
	a := appendA(t, s, 2)
	ctx := context.Background()
	if _, err := s.db.Exec(ctx, "UPDATE blocks SET canonical=false WHERE chain_id=31337 AND hash=$1", a[1].Block.Hash[:]); err != nil {
		t.Fatal(err)
	}
	state, err := s.Readiness(ctx, testChain)
	if err != nil || state.State != "unsafe" || state.Reason != "parent_chain_invalid" {
		t.Fatalf("readiness %+v %v", state, err)
	}
	if _, err = s.Init(ctx, testDataset()); !errors.Is(err, ErrCorruptCanonical) {
		t.Fatalf("init with corrupt checkpoint: %v", err)
	}
	if err = s.Append(ctx, testChain, testBatch(t, 3, 3, false)); !errors.Is(err, ErrCorruptCanonical) {
		t.Fatalf("append with corrupt checkpoint: %v", err)
	}
}
