//go:build integration

package multirpc

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"reorgguard/internal/backfill"
	"reorgguard/internal/model"
	"reorgguard/internal/reorg"
	"reorgguard/internal/rpc"
	"reorgguard/internal/store"
)

const testChain = 31337

var schemaSequence atomic.Uint64

func testHash(branch byte, n uint64) common.Hash {
	var h common.Hash
	h[22] = 1
	h[23] = branch
	binary.BigEndian.PutUint64(h[24:], n)
	return h
}
func testBlock(branch byte, n uint64, parent common.Hash) model.Block {
	return model.Block{Number: n, Hash: testHash(branch, n), ParentHash: parent, Time: time.Unix(int64(n+1), 0).UTC()}
}
func testFilter() rpc.Filter { return rpc.Filter{Addresses: []common.Address{{19: 1}}} }

type fixtureSource struct {
	mu            sync.Mutex
	chain         uint64
	head          uint64
	active        map[uint64]model.Block
	history       map[common.Hash]model.Block
	logs          map[common.Hash][]model.Log
	fail          map[string]error
	failCount     map[string]int // -1 means persistent
	failAt        map[string]int
	rangeFail     map[uint64]error
	maxRange      uint64
	calls         map[string]int
	ranges        [][2]uint64
	cancelOnChain context.CancelFunc
}

func newFixtureSource() *fixtureSource {
	f := &fixtureSource{chain: testChain, active: map[uint64]model.Block{}, history: map[common.Hash]model.Block{}, logs: map[common.Hash][]model.Log{}, fail: map[string]error{}, failCount: map[string]int{}, failAt: map[string]int{}, rangeFail: map[uint64]error{}, calls: map[string]int{}}
	f.setBranch(0, 0, 0)
	return f
}
func (f *fixtureSource) setBranch(head, fork uint64, branch byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.active = make(map[uint64]model.Block, head+1)
	parent := common.Hash{}
	for n := uint64(0); n <= head; n++ {
		id := byte(0)
		if n >= fork {
			id = branch
		}
		if n == 0 {
			id = 0
		}
		b := testBlock(id, n, parent)
		f.active[n] = b
		f.history[b.Hash] = b
		if n > 0 && n%2 == 1 {
			f.logs[b.Hash] = []model.Log{{BlockHash: b.Hash, BlockNumber: n, TxHash: testHash(id+100, n), Index: 0, Address: common.Address{19: 1}, Topics: []common.Hash{testHash(90, n)}, Data: []byte{id, byte(n)}}}
		}
		parent = b.Hash
	}
	f.head = head
}
func (f *fixtureSource) failMethod(method string, err error, count int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail[method] = err
	f.failCount[method] = count
}
func (f *fixtureSource) check(method string) error {
	f.calls[method]++
	if at := f.failAt[method]; at > 0 && f.calls[method] == at {
		return f.fail[method]
	}
	if err, ok := f.fail[method]; ok {
		n := f.failCount[method]
		if n != 0 {
			if n > 0 {
				f.failCount[method] = n - 1
			}
			return err
		}
	}
	return nil
}
func (f *fixtureSource) ChainID(ctx context.Context) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cancelOnChain != nil {
		f.cancelOnChain()
		return 0, ctx.Err()
	}
	if err := f.check("chain"); err != nil {
		return 0, err
	}
	return f.chain, nil
}
func TestPoolCancellationDoesNotProbeAnotherProvider(t *testing.T) {
	s, _ := testStore(t)
	a, b := newFixtureSource(), newFixtureSource()
	a.setBranch(3, 0, 0)
	b.setBranch(3, 0, 0)
	p := testPool(t, s, a, b)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a.cancelOnChain = cancel
	_, _, err := p.Sweep(ctx)
	if !errors.Is(err, context.Canceled) || b.count("chain") != 0 {
		t.Fatalf("cancellation probed fallback: %v calls=%d", err, b.count("chain"))
	}
}
func (f *fixtureSource) BlockNumber(context.Context) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.check("head"); err != nil {
		return 0, err
	}
	return f.head, nil
}
func (f *fixtureSource) BlockByNumber(_ context.Context, n uint64) (model.Block, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.check("block_number"); err != nil {
		return model.Block{}, err
	}
	if n > f.head {
		return model.Block{}, rpc.ErrNotFound
	}
	b, ok := f.active[n]
	if !ok {
		return model.Block{}, rpc.ErrNotFound
	}
	return b, nil
}
func (f *fixtureSource) BlockByHash(_ context.Context, h common.Hash) (model.Block, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.check("block_hash"); err != nil {
		return model.Block{}, err
	}
	b, ok := f.history[h]
	if !ok {
		return model.Block{}, rpc.ErrNotFound
	}
	return b, nil
}
func (f *fixtureSource) Logs(_ context.Context, from, to uint64, _ rpc.Filter) ([]model.Log, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.check("logs"); err != nil {
		return nil, err
	}
	f.ranges = append(f.ranges, [2]uint64{from, to})
	if err := f.rangeFail[from]; err != nil {
		return nil, err
	}
	if f.maxRange > 0 && to-from+1 > f.maxRange {
		return nil, &rpc.RPCError{Code: -32005, Message: "too many results"}
	}
	if to > f.head {
		return nil, rpc.ErrNotFound
	}
	var out []model.Log
	for n := from; n <= to; n++ {
		out = append(out, f.logs[f.active[n].Hash]...)
	}
	return out, nil
}
func (f *fixtureSource) LogsByBlockHash(_ context.Context, h common.Hash, _ rpc.Filter) ([]model.Log, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.check("logs_by_hash"); err != nil {
		return nil, err
	}
	return append([]model.Log(nil), f.logs[h]...), nil
}
func (f *fixtureSource) count(method string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[method]
}
func (f *fixtureSource) seenRanges() [][2]uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][2]uint64(nil), f.ranges...)
}

func testStore(t *testing.T) (*store.Store, string) {
	t.Helper()
	base := os.Getenv("REORGGUARD_TEST_DATABASE_URL")
	if base == "" {
		t.Skip("PostgreSQL DSN not provided")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("rg_pool_%d_%d", time.Now().UnixNano(), schemaSequence.Add(1))
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		_ = admin.Close(context.Background())
	})
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	dsn := u.String()
	s, err := store.Open(ctx, dsn, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s, dsn
}
func testPool(t *testing.T, s *store.Store, providers ...*fixtureSource) *Pool {
	t.Helper()
	endpoints := make([]Endpoint, len(providers))
	for i, source := range providers {
		id := "primary"
		if i > 0 {
			id = fmt.Sprintf("fallback_%d", i)
		}
		endpoints[i] = Endpoint{ID: id, Source: source}
	}
	runner := &backfill.Runner{ChainID: testChain, Genesis: testHash(0, 0), StartBlock: 1, Filter: testFilter(), Config: backfill.Config{InitialRange: 2, MinRange: 1, MaxRange: 8, MaxAttempts: 1, RangeTimeout: time.Second, RetryDelay: time.Millisecond, HealthyMaxLogs: 100, HealthyLatency: time.Second, GrowthAfter: 2, MaxLogsPerRange: 1000}}
	engine := &reorg.Engine{ChainID: testChain, Filter: testFilter(), MaxDepth: 5, Timeout: time.Second, MaxLogsPerBlock: 1000, Metrics: reorg.NewMetrics()}
	p, err := New(s, endpoints, Config{ChainID: testChain, Genesis: testHash(0, 0), Filter: testFilter(), ProbeTimeout: time.Second, Cooldown: time.Millisecond, MaxSwitches: len(endpoints), Runner: runner, Engine: engine})
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func checkpoint(t *testing.T, s *store.Store) store.Checkpoint {
	t.Helper()
	cp, err := s.Checkpoint(context.Background(), testChain)
	if err != nil {
		t.Fatal(err)
	}
	return cp
}
func sweep(t *testing.T, p *Pool) backfill.Result {
	t.Helper()
	res, _, err := p.Sweep(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return res
}
func canonicalChecksum(t *testing.T, dsn string) (string, int, int) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	h := sha256.New()
	blocks, logs := 0, 0
	rows, err := conn.Query(ctx, "SELECT number,hash,parent_hash FROM blocks WHERE chain_id=$1 AND canonical ORDER BY number", testChain)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var n int64
		var hash, parent []byte
		if err := rows.Scan(&n, &hash, &parent); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(h, "B|%d|%x|%x\n", n, hash, parent)
		blocks++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	rows, err = conn.Query(ctx, "SELECT l.block_number,l.block_hash,l.tx_hash,l.log_index,l.data FROM logs l JOIN blocks b ON b.chain_id=l.chain_id AND b.hash=l.block_hash AND b.canonical WHERE l.chain_id=$1 ORDER BY l.block_number,l.log_index", testChain)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var n int64
		var bh, tx, data []byte
		var index int32
		if err := rows.Scan(&n, &bh, &tx, &index, &data); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(h, "L|%d|%x|%x|%d|%x\n", n, bh, tx, index, data)
		logs++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	return hex.EncodeToString(h.Sum(nil)), blocks, logs
}

func TestPoolPrimaryAndGuardedFailover(t *testing.T) {
	for _, scenario := range []struct {
		name    string
		failure error
		method  string
	}{{"healthy_primary", nil, ""}, {"transport", errors.New("offline"), "head"}, {"timeout", context.DeadlineExceeded, "head"}, {"rate_limit", &rpc.RPCError{Code: -32005}, "logs"}} {
		t.Run(scenario.name, func(t *testing.T) {
			s, dsn := testStore(t)
			primary, fallback := newFixtureSource(), newFixtureSource()
			primary.setBranch(5, 0, 0)
			fallback.setBranch(5, 0, 0)
			p := testPool(t, s, primary, fallback)
			if scenario.name == "healthy_primary" {
				sweep(t, p)
				for _, rangeCall := range fallback.seenRanges() {
					if rangeCall[0] > 0 {
						t.Fatalf("fallback used for canonical range while primary healthy: %v", rangeCall)
					}
				}
				return
			}
			if scenario.name == "transport" {
				primary.setBranch(3, 0, 0)
				fallback.setBranch(3, 0, 0)
				first := sweep(t, p)
				if first.Checkpoint.Number != 3 || p.Status().Active != "primary" {
					t.Fatalf("initial primary progress %+v %+v", first, p.Status())
				}
				primary.setBranch(5, 0, 0)
				fallback.setBranch(5, 0, 0)
			}
			primary.failMethod(scenario.method, scenario.failure, -1)
			result := sweep(t, p)
			if result.Checkpoint.Number != 5 || p.Status().Active != "fallback_1" {
				t.Fatalf("failover %+v %+v", result, p.Status())
			}
			if _, blocks, logs := canonicalChecksum(t, dsn); blocks != 6 || logs != 3 {
				t.Fatalf("projection blocks=%d logs=%d", blocks, logs)
			}
			if scenario.name == "transport" {
				for _, call := range fallback.seenRanges() {
					if call[0] > 3 && call[0] != 4 {
						t.Fatalf("fallback skipped durable checkpoint: %v", fallback.seenRanges())
					}
				}
			}
		})
	}
}
func TestPoolBoundedMetricsAndReadinessSignals(t *testing.T) {
	s, _ := testStore(t)
	a, b := newFixtureSource(), newFixtureSource()
	a.setBranch(3, 0, 0)
	b.setBranch(3, 0, 0)
	p := testPool(t, s, a, b)
	sweep(t, p)
	a.failMethod("head", errors.New("credential=very-secret"), -1)
	b.setBranch(5, 0, 0)
	sweep(t, p)
	status := p.Status()
	if status.Active != "fallback_1" || status.Failovers != 1 || status.FailoverReasons["transport"] != 1 || status.Endpoints[1].State != "healthy" || status.Endpoints[0].State != "temporarily_unavailable" {
		t.Fatalf("health state %+v", status)
	}
	var output strings.Builder
	if err := p.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	got := output.String()
	if strings.Contains(got, "very-secret") || strings.Contains(got, "credential") || !strings.Contains(got, `endpoint="fallback_1"`) || !strings.Contains(got, `reorgguard_rpc_failovers_total 1`) {
		t.Fatalf("metric labels or counters incorrect: %s", got)
	}
}
func TestPoolStaleAndBehindCheckpoint(t *testing.T) {
	s, _ := testStore(t)
	primary, fallback := newFixtureSource(), newFixtureSource()
	primary.setBranch(3, 0, 0)
	fallback.setBranch(3, 0, 0)
	p := testPool(t, s, primary, fallback)
	sweep(t, p)
	primary.setBranch(2, 0, 0)
	fallback.setBranch(5, 0, 0)
	res := sweep(t, p)
	if res.Checkpoint.Number != 5 || p.Status().Active != "fallback_1" || p.Status().StaleDetections == 0 {
		t.Fatalf("stale failover %+v %+v", res, p.Status())
	}
}
func TestPoolHardRejectsBadFallback(t *testing.T) {
	for _, kind := range []string{"wrong_chain", "wrong_genesis", "checkpoint_mismatch", "changed_checkpoint_identity", "changed_checkpoint_logs", "missing_checkpoint", "missing_method", "missing_range_method"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := testStore(t)
			primary, fallback := newFixtureSource(), newFixtureSource()
			primary.setBranch(3, 0, 0)
			fallback.setBranch(3, 0, 0)
			p := testPool(t, s, primary, fallback)
			sweep(t, p)
			primary.failMethod("head", errors.New("offline"), -1)
			switch kind {
			case "wrong_chain":
				fallback.chain = 1
			case "wrong_genesis":
				fallback.active[0] = testBlock(9, 0, common.Hash{})
			case "checkpoint_mismatch":
				fallback.setBranch(4, 3, 9)
			case "changed_checkpoint_identity":
				bad := fallback.active[3]
				bad.Time = bad.Time.Add(time.Second)
				fallback.active[3] = bad
				fallback.history[bad.Hash] = bad
			case "changed_checkpoint_logs":
				logs := fallback.logs[fallback.active[3].Hash]
				logs[0].Data = []byte("changed")
				fallback.logs[fallback.active[3].Hash] = logs
			case "missing_checkpoint":
				fallback.failMethod("block_number", rpc.ErrNotFound, -1)
			case "missing_method":
				fallback.failMethod("logs_by_hash", &rpc.RPCError{Code: -32601}, -1)
			case "missing_range_method":
				fallback.failMethod("logs", &rpc.RPCError{Code: -32601}, -1)
			}
			_, _, err := p.Sweep(context.Background())
			if err == nil {
				t.Fatal("bad fallback selected")
			}
			if cp := checkpoint(t, s); cp.Number != 3 || cp.Hash != testHash(0, 3) {
				t.Fatalf("checkpoint moved %+v", cp)
			}
			state := p.Status().Endpoints[1]
			if kind != "missing_checkpoint" && state.State != "incompatible" {
				t.Fatalf("fallback not hard rejected %+v", state)
			}
		})
	}
}
func TestPoolRejectsWrongConfiguredStartAnchor(t *testing.T) {
	s, _ := testStore(t)
	a, b := newFixtureSource(), newFixtureSource()
	a.setBranch(4, 0, 0)
	b.setBranch(4, 0, 0)
	bad := b.active[2]
	bad.Hash = testHash(9, 2)
	b.active[2] = bad
	b.history[bad.Hash] = bad
	runner := &backfill.Runner{ChainID: testChain, Genesis: testHash(0, 0), StartBlock: 3, AnchorHash: testHash(0, 2), Filter: testFilter(), Config: backfill.Config{InitialRange: 2, MinRange: 1, MaxRange: 8, MaxAttempts: 2, RangeTimeout: time.Second, HealthyLatency: time.Second, GrowthAfter: 2, MaxLogsPerRange: 100}}
	engine := &reorg.Engine{ChainID: testChain, Filter: testFilter(), MaxDepth: 5, Timeout: time.Second, MaxLogsPerBlock: 100}
	p, err := New(s, []Endpoint{{ID: "primary", Source: a}, {ID: "fallback_1", Source: b}}, Config{ChainID: testChain, Genesis: testHash(0, 0), Filter: testFilter(), ProbeTimeout: time.Second, Cooldown: time.Millisecond, MaxSwitches: 2, Runner: runner, Engine: engine})
	if err != nil {
		t.Fatal(err)
	}
	sweep(t, p)
	if p.Status().Endpoints[1].State != "incompatible" || p.Status().Endpoints[1].Reason != "wrong_anchor" || checkpoint(t, s).Number != 4 {
		t.Fatalf("wrong anchor accepted: %+v", p.Status())
	}
}
func TestPoolMidRangeFailureAndAdaptivePerEndpoint(t *testing.T) {
	s, dsn := testStore(t)
	primary, fallback := newFixtureSource(), newFixtureSource()
	primary.setBranch(8, 0, 0)
	fallback.setBranch(8, 0, 0)
	primary.rangeFail[3] = errors.New("midrange offline")
	fallback.maxRange = 1
	p := testPool(t, s, primary, fallback)
	p.endpoints[1].runner.Config.MaxAttempts = 4
	res := sweep(t, p)
	if res.Checkpoint.Number != 8 || checkpoint(t, s).Number != 8 || p.Status().Active != "fallback_1" {
		t.Fatalf("midrange failover %+v %+v", res, p.Status())
	}
	ranges := fallback.seenRanges()
	saw2, saw1 := false, false
	for _, v := range ranges {
		if v == ([2]uint64{3, 4}) {
			saw2 = true
		}
		if v == ([2]uint64{3, 3}) {
			saw1 = true
		}
	}
	if !saw2 || !saw1 {
		t.Fatalf("fallback did not shrink same range: %v", ranges)
	}
	if _, blocks, logs := canonicalChecksum(t, dsn); blocks != 9 || logs != 4 {
		t.Fatalf("projection blocks=%d logs=%d", blocks, logs)
	}
}
func TestPoolNoConsensusFromProviderCount(t *testing.T) {
	s, dsn := testStore(t)
	primary, fallback := newFixtureSource(), newFixtureSource()
	primary.setBranch(4, 0, 0)
	fallback.setBranch(4, 0, 0)
	p := testPool(t, s, primary, fallback)
	sweep(t, p)
	before, _, _ := canonicalChecksum(t, dsn)
	primary.failMethod("head", errors.New("offline"), -1)
	fallback.setBranch(5, 3, 8) // same chain/genesis, incompatible checkpoint hash
	_, _, err := p.Sweep(context.Background())
	if err == nil {
		t.Fatal("provider disagreement accepted as consensus")
	}
	after, _, _ := canonicalChecksum(t, dsn)
	if before != after || checkpoint(t, s).Number != 4 {
		t.Fatal("checkpoint changed on unproven provider branch")
	}
	if p.Status().Endpoints[1].State != "incompatible" {
		t.Fatalf("fallback status %+v", p.Status())
	}
}
func TestPoolDuplicateAndMalformedProviderData(t *testing.T) {
	s, dsn := testStore(t)
	primary, fallback := newFixtureSource(), newFixtureSource()
	primary.setBranch(5, 0, 0)
	fallback.setBranch(5, 0, 0)
	duplicate := fallback.logs[fallback.active[1].Hash][0]
	fallback.logs[fallback.active[1].Hash] = append(fallback.logs[fallback.active[1].Hash], duplicate)
	p := testPool(t, s, primary, fallback)
	primary.failMethod("block_number", rpc.ErrMalformed, -1)
	res := sweep(t, p)
	if res.Checkpoint.Number != 5 {
		t.Fatal("malformed provider advanced")
	}
	first, blocks, logs := canonicalChecksum(t, dsn)
	res = sweep(t, p)
	second, b2, l2 := canonicalChecksum(t, dsn)
	if first != second || blocks != b2 || logs != l2 || res.Ranges != 0 {
		t.Fatal("duplicate canonical records")
	}
}
func TestPoolMalformedLogFailsWithoutCanonicalCorruption(t *testing.T) {
	s, dsn := testStore(t)
	a, b := newFixtureSource(), newFixtureSource()
	a.setBranch(5, 0, 0)
	b.setBranch(5, 0, 0)
	bad := a.logs[a.active[3].Hash][0]
	bad.BlockNumber = 99
	a.logs[a.active[3].Hash] = []model.Log{bad}
	b.logs[b.active[3].Hash] = []model.Log{bad}
	p := testPool(t, s, a, b)
	_, _, err := p.Sweep(context.Background())
	if !errors.Is(err, model.ErrInvalidRange) || checkpoint(t, s).Number != 2 {
		t.Fatalf("malformed log accepted: %v %+v", err, checkpoint(t, s))
	}
	if _, blocks, logs := canonicalChecksum(t, dsn); blocks != 3 || logs != 1 {
		t.Fatalf("partial canonical corruption blocks=%d logs=%d", blocks, logs)
	}
}
func TestPoolRecoveryAndFlappingBounded(t *testing.T) {
	s, _ := testStore(t)
	primary, fallback := newFixtureSource(), newFixtureSource()
	primary.setBranch(4, 0, 0)
	fallback.setBranch(4, 0, 0)
	p := testPool(t, s, primary, fallback)
	primary.failMethod("head", errors.New("offline"), -1)
	sweep(t, p)
	if p.Status().Active != "fallback_1" {
		t.Fatal("fallback not selected")
	}
	primary.failMethod("head", nil, 0)
	primary.setBranch(5, 0, 0)
	fallback.setBranch(5, 0, 0)
	fallback.failMethod("head", errors.New("fallback offline"), -1)
	p.mu.Lock()
	p.endpoints[0].health.CooldownUntil = time.Time{}
	p.mu.Unlock()
	sweep(t, p)
	if p.Status().Active != "primary" {
		t.Fatalf("primary did not recover %+v", p.Status())
	}
	if p.Status().Failovers > 2 {
		t.Fatal("unbounded switching")
	}
}
func TestPoolAllUnavailable(t *testing.T) {
	s, _ := testStore(t)
	a, b := newFixtureSource(), newFixtureSource()
	a.setBranch(3, 0, 0)
	b.setBranch(3, 0, 0)
	p := testPool(t, s, a, b)
	sweep(t, p)
	a.failMethod("head", errors.New("offline"), -1)
	b.failMethod("head", errors.New("offline"), -1)
	_, _, err := p.Sweep(context.Background())
	if !errors.Is(err, ErrNoEligible) || checkpoint(t, s).Number != 3 {
		t.Fatalf("all unavailable %v %+v", err, checkpoint(t, s))
	}
}

func TestPoolDifferentNewerBranchesNoVoting(t *testing.T) {
	s, dsn := testStore(t)
	a, b := newFixtureSource(), newFixtureSource()
	a.setBranch(2, 0, 0)
	b.setBranch(2, 0, 0)
	p := testPool(t, s, a, b)
	sweep(t, p)
	a.setBranch(4, 0, 0)
	b.setBranch(4, 3, 8) // both verified at the local checkpoint 2
	sweep(t, p)
	if checkpoint(t, s).Hash != testHash(0, 4) || p.Status().Active != "primary" {
		t.Fatalf("selected branch without local continuity: %+v", p.Status())
	}
	before, _, _ := canonicalChecksum(t, dsn)
	a.failMethod("head", errors.New("primary offline"), -1)
	_, _, err := p.Sweep(context.Background())
	if err == nil {
		t.Fatal("fallback branch voted canonical")
	}
	after, _, _ := canonicalChecksum(t, dsn)
	if before != after {
		t.Fatal("canonical branch changed by provider disagreement")
	}
}
func TestPoolFailoverDuringReorgDetection(t *testing.T) {
	s, _ := testStore(t)
	a, b := newFixtureSource(), newFixtureSource()
	a.setBranch(4, 0, 0)
	b.setBranch(4, 0, 0)
	p := testPool(t, s, a, b)
	sweep(t, p)
	a.setBranch(4, 3, 8)
	a.mu.Lock()
	a.fail["block_hash"] = errors.New("lookup connection lost")
	a.failCount["block_hash"] = 0
	a.failAt["block_hash"] = a.calls["block_hash"] + 2
	a.mu.Unlock()
	sweep(t, p)
	if p.Status().Active != "fallback_1" || checkpoint(t, s).Hash != testHash(0, 4) {
		t.Fatalf("reorg lookup failover %+v %+v", p.Status(), checkpoint(t, s))
	}
}
func TestPoolABAReturnAcrossEndpointChange(t *testing.T) {
	s, dsn := testStore(t)
	a, b := newFixtureSource(), newFixtureSource()
	a.setBranch(4, 0, 0)
	b.setBranch(4, 0, 0)
	p := testPool(t, s, a, b)
	sweep(t, p)
	reference, _, _ := canonicalChecksum(t, dsn)
	a.setBranch(4, 3, 8)
	sweep(t, p) // active primary proves A -> B; fallback still at A while old checkpoint was A
	if checkpoint(t, s).Hash != testHash(8, 4) {
		t.Fatal("A to B failed")
	}
	b.setBranch(4, 3, 8)
	a.failMethod("head", errors.New("primary offline"), -1)
	sweep(t, p)
	if p.Status().Active != "fallback_1" {
		t.Fatal("fallback did not take over B")
	}
	b.setBranch(4, 0, 0)
	sweep(t, p) // active fallback proves B -> A
	actual, blocks, logs := canonicalChecksum(t, dsn)
	if actual != reference || blocks != 5 || logs != 2 || checkpoint(t, s).Hash != testHash(0, 4) {
		t.Fatalf("A-B-A projection mismatch checksum=%s/%s blocks=%d logs=%d", actual, reference, blocks, logs)
	}
}
func TestPoolCorruptParentAndMissingIntermediateFailClosed(t *testing.T) {
	for _, kind := range []string{"corrupt_parent", "missing_intermediate"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := testStore(t)
			a, b := newFixtureSource(), newFixtureSource()
			a.setBranch(2, 0, 0)
			b.setBranch(2, 0, 0)
			p := testPool(t, s, a, b)
			sweep(t, p)
			a.failMethod("head", errors.New("primary offline"), -1)
			b.setBranch(5, 0, 0)
			b.mu.Lock()
			if kind == "corrupt_parent" {
				bad := b.active[3]
				bad.ParentHash = testHash(99, 99)
				b.active[3] = bad
				b.history[bad.Hash] = bad
			} else {
				delete(b.active, 3)
			}
			b.mu.Unlock()
			_, _, err := p.Sweep(context.Background())
			if err == nil || checkpoint(t, s).Number != 2 {
				t.Fatalf("bad provider history accepted: %v %+v", err, checkpoint(t, s))
			}
		})
	}
}
func TestPoolChangedChainIDAndAllIncompatible(t *testing.T) {
	s, _ := testStore(t)
	a, b := newFixtureSource(), newFixtureSource()
	a.setBranch(3, 0, 0)
	b.setBranch(3, 0, 0)
	p := testPool(t, s, a, b)
	sweep(t, p)
	a.mu.Lock()
	a.chain = 1
	a.mu.Unlock()
	b.setBranch(4, 0, 0)
	sweep(t, p)
	if p.Status().Active != "fallback_1" || p.Status().Endpoints[0].State != "incompatible" {
		t.Fatalf("changed primary chain not rejected %+v", p.Status())
	}
	b.mu.Lock()
	b.chain = 2
	b.mu.Unlock()
	_, _, err := p.Sweep(context.Background())
	if !errors.Is(err, ErrIncompatible) {
		t.Fatalf("all incompatible did not fail closed: %v", err)
	}
}
func TestPoolImmutableBlockIdentityConflict(t *testing.T) {
	s, _ := testStore(t)
	a, b := newFixtureSource(), newFixtureSource()
	a.setBranch(4, 0, 0)
	b.setBranch(4, 0, 0)
	p := testPool(t, s, a, b)
	sweep(t, p)
	a.setBranch(4, 3, 8)
	sweep(t, p)
	a.setBranch(4, 0, 0)
	a.mu.Lock()
	bad := a.active[3]
	bad.Time = bad.Time.Add(time.Second)
	a.active[3] = bad
	a.history[bad.Hash] = bad
	a.mu.Unlock()
	_, _, err := p.Sweep(context.Background())
	if !errors.Is(err, model.ErrIdentity) || checkpoint(t, s).Hash != testHash(8, 4) {
		t.Fatalf("changed historical identity accepted: %v %+v", err, checkpoint(t, s))
	}
}
func TestPoolParallelFailoverOneInFlightRange(t *testing.T) {
	s, dsn := testStore(t)
	a, b := newFixtureSource(), newFixtureSource()
	a.setBranch(8, 0, 0)
	b.setBranch(8, 0, 0)
	a.rangeFail[3] = errors.New("parallel range offline")
	p := testPool(t, s, a, b)
	for _, e := range p.endpoints {
		e.runner.Config.Workers = 3
		e.runner.Config.MaxInFlight = 4
		e.runner.Config.InitialRange = 2
		e.runner.Config.MaxRange = 2
	}
	result := sweep(t, p)
	if result.Checkpoint.Number != 8 || p.Status().Active != "fallback_1" {
		t.Fatalf("parallel failover %+v %+v", result, p.Status())
	}
	parallelChecksum, blocks, logs := canonicalChecksum(t, dsn)
	if blocks != 9 || logs != 4 {
		t.Fatalf("parallel projection %d/%d", blocks, logs)
	}
	referenceStore, referenceDSN := testStore(t)
	referenceSource := newFixtureSource()
	referenceSource.setBranch(8, 0, 0)
	sweep(t, testPool(t, referenceStore, referenceSource))
	serialChecksum, serialBlocks, serialLogs := canonicalChecksum(t, referenceDSN)
	if parallelChecksum != serialChecksum || blocks != serialBlocks || logs != serialLogs {
		t.Fatalf("parallel projection differs from serial: checksum=%s/%s blocks=%d/%d logs=%d/%d", parallelChecksum, serialChecksum, blocks, serialBlocks, logs, serialLogs)
	}
	for _, e := range p.Status().Endpoints {
		if e.Backfill.MaxActive > 3 || e.Backfill.MaxInFlight > 4 || e.Backfill.Active != 0 || e.Backfill.InFlight != 0 || e.Backfill.Pending != 0 {
			t.Fatalf("parallel bounds %+v", e.Backfill)
		}
	}
}
func TestPoolPerEndpointFiveThousandBlockCapability(t *testing.T) {
	if testing.Short() {
		t.Skip("large capability fixture")
	}
	s, _ := testStore(t)
	a, b := newFixtureSource(), newFixtureSource()
	a.setBranch(5000, 0, 0)
	b.setBranch(5000, 0, 0)
	b.maxRange = 1000
	p := testPool(t, s, a, b)
	for _, e := range p.endpoints {
		e.runner.Config.InitialRange = 5000
		e.runner.Config.MaxRange = 5000
		e.runner.Config.MaxAttempts = 8
		e.runner.Config.MaxLogsPerRange = 10000
		e.runner.Config.RangeTimeout = 30 * time.Second
	}
	if result := sweep(t, p); result.Checkpoint.Number != 5000 {
		t.Fatalf("primary 5000 range %+v", result)
	}
	a.failMethod("head", errors.New("primary offline"), -1)
	b.setBranch(10000, 0, 0)
	if result := sweep(t, p); result.Checkpoint.Number != 10000 {
		t.Fatalf("fallback capability %+v", result)
	}
	ranges := b.seenRanges()
	large, small := false, false
	for _, v := range ranges {
		if v[0] == 5001 && v[1] == 10000 {
			large = true
		}
		if v[0] == 5001 && v[1]-v[0]+1 <= 1000 {
			small = true
		}
	}
	if !large || !small || checkpoint(t, s).Number != 10000 {
		t.Fatalf("per-provider range adaptation absent: %v", ranges)
	}
	status := p.Status()
	if status.Endpoints[0].AdaptiveWindow != 5000 || status.Endpoints[1].AdaptiveWindow > 1000 || status.Endpoints[1].LimitResponses == 0 {
		t.Fatalf("per-endpoint capability state %+v", status.Endpoints)
	}
}
