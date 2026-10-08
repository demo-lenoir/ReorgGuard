//go:build integration

package reorg

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"reorgguard/internal/backfill"
	"reorgguard/internal/model"
	"reorgguard/internal/rpc"
	"reorgguard/internal/store"
)

var schemaCounter atomic.Uint64

func hash(n byte) common.Hash { var h common.Hash; h[31] = n; return h }
func block(n uint64, id byte, parent common.Hash) model.Block {
	return model.Block{Number: n, Hash: hash(id), ParentHash: parent, Time: time.Unix(int64(n+1), 0).UTC()}
}

type fixture struct {
	byNumber       map[uint64]model.Block
	byHash         map[common.Hash]model.Block
	logs           map[common.Hash][]model.Log
	blockHashCalls int
	logCalls       int
	cancelOnHash   bool
	cancelOnLogs   bool
	cancelHash     common.Hash
	cancelLogsHash common.Hash
}

func newFixture() *fixture {
	f := &fixture{byNumber: map[uint64]model.Block{}, byHash: map[common.Hash]model.Block{}, logs: map[common.Hash][]model.Log{}}
	f.add(block(0, 1, common.Hash{}), false)
	return f
}
func (f *fixture) add(b model.Block, withLog bool) {
	f.byNumber[b.Number] = b
	f.byHash[b.Hash] = b
	if withLog {
		f.logs[b.Hash] = []model.Log{{BlockHash: b.Hash, BlockNumber: b.Number, TxHash: hash(byte(180 + b.Number)), Index: 0, Address: common.Address{19: 1}, Topics: []common.Hash{hash(91)}, Data: []byte{b.Hash[31]}}}
	}
}
func (f *fixture) chain(end uint64, forkFrom uint64, branch byte) []model.Block {
	var out []model.Block
	parent := f.byNumber[forkFrom-1].Hash
	for n := forkFrom; n <= end; n++ {
		b := block(n, branch+byte(n), parent)
		f.add(b, n%2 == 1)
		out = append(out, b)
		parent = b.Hash
	}
	return out
}
func (f *fixture) ChainID(context.Context) (uint64, error) { return 31337, nil }
func (f *fixture) BlockNumber(context.Context) (uint64, error) {
	var max uint64
	for n := range f.byNumber {
		if n > max {
			max = n
		}
	}
	return max, nil
}
func (f *fixture) BlockByNumber(ctx context.Context, n uint64) (model.Block, error) {
	if err := ctx.Err(); err != nil {
		return model.Block{}, err
	}
	b, ok := f.byNumber[n]
	if !ok {
		return model.Block{}, rpc.ErrNotFound
	}
	return b, nil
}
func (f *fixture) BlockByHash(ctx context.Context, h common.Hash) (model.Block, error) {
	f.blockHashCalls++
	if f.cancelOnHash && h == f.cancelHash {
		<-ctx.Done()
		return model.Block{}, ctx.Err()
	}
	b, ok := f.byHash[h]
	if !ok {
		return model.Block{}, rpc.ErrNotFound
	}
	return b, nil
}
func (f *fixture) Logs(ctx context.Context, from, to uint64, _ rpc.Filter) ([]model.Log, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var out []model.Log
	for n := from; n <= to; n++ {
		out = append(out, f.logs[f.byNumber[n].Hash]...)
	}
	return out, nil
}
func (f *fixture) LogsByBlockHash(ctx context.Context, h common.Hash, _ rpc.Filter) ([]model.Log, error) {
	f.logCalls++
	if f.cancelOnLogs && h == f.cancelLogsHash {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return append([]model.Log(nil), f.logs[h]...), nil
}

func openTestStore(t *testing.T) (*store.Store, *pgxpool.Config) {
	t.Helper()
	dsn := os.Getenv("REORGGUARD_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("REORGGUARD_TEST_DATABASE_URL required")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("rg_reorg_%d_%d", time.Now().UnixNano(), schemaCounter.Add(1))
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	s, err := store.OpenConfig(ctx, cfg, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		s.Close()
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		admin.Close(context.Background())
	})
	return s, cfg
}
func coordinator(f *fixture, s *store.Store, maxDepth uint64) *Coordinator {
	filter := rpc.Filter{Addresses: []common.Address{{19: 1}}}
	r := &backfill.Runner{Source: f, Store: s, ChainID: 31337, Genesis: hash(1), StartBlock: 1, Filter: filter, Config: backfill.Config{InitialRange: 3, MinRange: 1, MaxRange: 8, MaxAttempts: 3, RangeTimeout: time.Second, HealthyLatency: time.Second, HealthyMaxLogs: 20, GrowthAfter: 2, MaxLogsPerRange: 100}}
	e := &Engine{Source: f, Store: s, ChainID: 31337, Filter: filter, MaxDepth: maxDepth, Timeout: 500 * time.Millisecond, MaxLogsPerBlock: 100, Metrics: NewMetrics()}
	return &Coordinator{Backfill: r, Engine: e}
}
func requireHead(t *testing.T, s *store.Store, b model.Block) {
	t.Helper()
	cp, err := s.Checkpoint(context.Background(), 31337)
	if err != nil || cp.Number != b.Number || cp.Hash != b.Hash {
		t.Fatalf("head %+v %v want %d/%s", cp, err, b.Number, b.Hash)
	}
}

func TestCoordinatorAppendReorgABAAndRestart(t *testing.T) {
	s, cfg := openTestStore(t)
	f := newFixture()
	a := f.chain(4, 1, 1)
	c := coordinator(f, s, 2)
	res, err := c.Run(context.Background())
	if err != nil || res.Checkpoint.Hash != a[3].Hash {
		t.Fatalf("clean append %+v %v", res, err)
	}
	for _, item := range a {
		got, lookupErr := s.CanonicalBlockByNumber(context.Background(), 31337, item.Number)
		if lookupErr != nil || got.Hash != item.Hash {
			t.Fatalf("clean append height %d: %v", item.Number, lookupErr)
		}
	}
	b := f.chain(4, 3, 100)
	res, err = c.Run(context.Background())
	if err != nil || res.Checkpoint.Hash != b[1].Hash {
		t.Fatalf("A to B %+v %v", res, err)
	}
	state, err := c.Engine.Readiness(context.Background())
	if err != nil || state.State != "healthy" {
		t.Fatalf("readiness %+v %v", state, err)
	}
	// Direct backfill now rechecks the immutable checkpoint by hash and log
	// set on each pass; these exact counts include those boundary reads.
	if f.blockHashCalls != 5 || f.logCalls != 4 {
		t.Fatalf("parent/log fetches %d/%d", f.blockHashCalls, f.logCalls)
	}
	// Reopen after a complete switch, then reactivate exactly the original A.
	reopened, err := store.OpenConfig(context.Background(), cfg, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for _, item := range a[2:] {
		f.byNumber[item.Number] = item
	}
	c2 := coordinator(f, reopened, 2)
	res, err = c2.Run(context.Background())
	if err != nil || res.Checkpoint.Hash != a[3].Hash {
		t.Fatalf("B to A %+v %v", res, err)
	}
	for _, item := range a {
		got, lookupErr := reopened.CanonicalBlockByNumber(context.Background(), 31337, item.Number)
		if lookupErr != nil || got.Hash != item.Hash {
			t.Fatalf("A height %d: %v", item.Number, lookupErr)
		}
	}
	logs, err := reopened.CanonicalLogs(context.Background(), 31337, 1, 4)
	if err != nil || len(logs) != 2 || logs[1].BlockHash != a[2].Hash {
		t.Fatalf("A logs %+v %v", logs, err)
	}
	res, err = c2.Run(context.Background())
	if err != nil || res.Checkpoint.Hash != a[3].Hash {
		t.Fatalf("duplicate run %+v %v", res, err)
	}
	// Keep branch audit history: genesis + A1..A4 + B3..B4.
	for _, item := range b {
		stored, lookupErr := reopened.BlockByHash(context.Background(), 31337, item.Hash)
		if lookupErr != nil || stored.Canonical {
			t.Fatalf("orphaned B %+v %v", stored, lookupErr)
		}
	}
}

func TestEngineDepthMissingAncestorAndCorruptParents(t *testing.T) {
	for _, tc := range []struct {
		name    string
		depth   uint64
		corrupt bool
		missing bool
		want    error
		reason  string
	}{
		{"exact depth", 2, false, false, nil, ""},
		{"too deep", 1, false, false, ErrDepth, "depth_exceeded"},
		{"bad parent", 2, true, false, ErrParentChain, "parent_chain_invalid"},
		{"missing retained ancestor", 2, false, true, ErrAncestorMissing, "ancestor_missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, cfg := openTestStore(t)
			f := newFixture()
			a := f.chain(4, 1, 1)
			c := coordinator(f, s, tc.depth)
			if _, err := c.Run(context.Background()); err != nil {
				t.Fatal(err)
			}
			b := f.chain(4, 3, 100)
			if tc.corrupt {
				bad := b[1]
				bad.ParentHash = hash(250)
				f.byNumber[4] = bad
				f.byHash[bad.Hash] = bad
				f.byHash[hash(250)] = block(2, 250, hash(1)) // plausible hash, impossible height link
			}
			if tc.missing { // A2 remains the true parent remotely but is outside retained local history.
				conn, connErr := pgx.ConnectConfig(context.Background(), cfg.ConnConfig)
				if connErr != nil {
					t.Fatal(connErr)
				}
				_, connErr = conn.Exec(context.Background(), "DELETE FROM blocks WHERE chain_id=31337 AND hash=$1", a[1].Hash[:])
				conn.Close(context.Background())
				if connErr != nil {
					t.Fatal(connErr)
				}
			}
			_, err := c.Run(context.Background())
			if tc.want == nil {
				if err != nil {
					t.Fatal(err)
				}
				requireHead(t, s, b[1])
				return
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("want %v got %v", tc.want, err)
			}
			requireHead(t, s, a[3])
			state, readErr := c.Engine.Readiness(context.Background())
			if readErr != nil || state.State != "unsafe" || state.Reason != tc.reason {
				t.Fatalf("readiness %+v %v", state, readErr)
			}
			if _, err = c.Backfill.RunTo(context.Background(), 4); !errors.Is(err, store.ErrUnsafe) {
				t.Fatalf("durable unsafe: %v", err)
			}
		})
	}
}

func TestEngineCancellationKeepsCheckpoint(t *testing.T) {
	for _, which := range []string{"parent", "logs"} {
		t.Run(which, func(t *testing.T) {
			s, _ := openTestStore(t)
			f := newFixture()
			a := f.chain(3, 1, 1)
			c := coordinator(f, s, 2)
			if _, err := c.Run(context.Background()); err != nil {
				t.Fatal(err)
			}
			f.chain(3, 3, 100)
			if which == "parent" {
				f.cancelOnHash = true
				f.cancelHash = a[1].Hash
			} else {
				f.cancelOnLogs = true
				f.cancelLogsHash = f.byNumber[3].Hash
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			_, err := c.Run(ctx)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("cancel %v", err)
			}
			requireHead(t, s, a[2])
			state, readErr := c.Engine.Readiness(context.Background())
			if readErr != nil || state.State != "unsafe" {
				t.Fatalf("readiness %+v %v", state, readErr)
			}
			// A canceled, uncommitted attempt remains unhealthy for observation,
			// but a later HTTP sweep may retry from the same durable head.
			f.cancelOnHash = false
			f.cancelOnLogs = false
			if _, err := c.Run(context.Background()); err != nil {
				t.Fatalf("retry after transient cancellation: %v", err)
			}
			state, readErr = c.Engine.Readiness(context.Background())
			if readErr != nil || state.State != "healthy" {
				t.Fatalf("readiness after retry %+v %v", state, readErr)
			}
		})
	}
}

func TestEngineReadinessTransitions(t *testing.T) {
	s, _ := openTestStore(t)
	f := newFixture()
	f.chain(1, 1, 1)
	c := coordinator(f, s, 1)
	if _, err := c.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, err := c.Engine.Readiness(context.Background())
	if err != nil || state.State != "healthy" {
		t.Fatalf("healthy %+v %v", state, err)
	}
	if err = c.Engine.start(); err != nil {
		t.Fatal(err)
	}
	state, err = c.Engine.Readiness(context.Background())
	if err != nil || state.State != "reconciling" {
		t.Fatalf("reconciling %+v %v", state, err)
	}
	c.Engine.healthy()
	if err = s.MarkUnsafe(context.Background(), 31337, "depth_exceeded"); err != nil {
		t.Fatal(err)
	}
	state, err = c.Engine.Readiness(context.Background())
	if err != nil || state.State != "unsafe" || state.Reason != "depth_exceeded" {
		t.Fatalf("durable unsafe %+v %v", state, err)
	}
}
