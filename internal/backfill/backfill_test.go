package backfill

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"reorgguard/internal/model"
	"reorgguard/internal/rpc"
	"reorgguard/internal/store"
)

func h(n byte) common.Hash { var x common.Hash; x[31] = n; return x }

type source struct {
	chain  uint64
	head   uint64
	blocks []model.Block
	logs   func(context.Context, uint64, uint64) ([]model.Log, error)
}

func (s *source) BlockByHash(_ context.Context, hash common.Hash) (model.Block, error) {
	for _, block := range s.blocks {
		if block.Hash == hash {
			return block, nil
		}
	}
	return model.Block{}, rpc.ErrNotFound
}
func (s *source) LogsByBlockHash(ctx context.Context, hash common.Hash, _ rpc.Filter) ([]model.Log, error) {
	for _, block := range s.blocks {
		if block.Hash == hash {
			return s.Logs(ctx, block.Number, block.Number, rpc.Filter{})
		}
	}
	return nil, rpc.ErrNotFound
}

func (s *source) ChainID(context.Context) (uint64, error)     { return s.chain, nil }
func (s *source) BlockNumber(context.Context) (uint64, error) { return s.head, nil }
func (s *source) BlockByNumber(_ context.Context, n uint64) (model.Block, error) {
	if n >= uint64(len(s.blocks)) {
		return model.Block{}, errors.New("missing block")
	}
	return s.blocks[n], nil
}
func (s *source) Logs(ctx context.Context, from, to uint64, _ rpc.Filter) ([]model.Log, error) {
	if s.logs != nil {
		return s.logs(ctx, from, to)
	}
	return nil, nil
}

type repository struct {
	cp      store.Checkpoint
	blocks  map[uint64]common.Hash
	records map[uint64]model.Block
	logs    map[uint64][]model.Log
	calls   [][2]uint64
}

func (s *repository) Init(_ context.Context, d store.Dataset) (store.Checkpoint, error) {
	if s.blocks == nil {
		s.blocks = map[uint64]common.Hash{d.Anchor.Number: d.Anchor.Hash}
		s.records = map[uint64]model.Block{d.Anchor.Number: d.Anchor}
		s.logs = make(map[uint64][]model.Log)
		s.cp = store.Checkpoint{Number: d.Anchor.Number, Hash: d.Anchor.Hash}
	}
	return s.cp, nil
}
func (s *repository) Append(_ context.Context, _ uint64, items []model.BlockData) error {
	first := items[0].Block.Number
	last := items[len(items)-1].Block.Number
	if first != s.cp.Number+1 {
		return fmt.Errorf("gap at %d", first)
	}
	for _, item := range items {
		if _, exists := s.blocks[item.Block.Number]; exists {
			return errors.New("overlap")
		}
		s.blocks[item.Block.Number] = item.Block.Hash
		s.records[item.Block.Number] = item.Block
		s.logs[item.Block.Number] = append([]model.Log(nil), item.Logs...)
	}
	s.cp = store.Checkpoint{Number: last, Hash: items[len(items)-1].Block.Hash}
	s.calls = append(s.calls, [2]uint64{first, last})
	return nil
}
func (s *repository) CanonicalBlockByNumber(_ context.Context, _ uint64, number uint64) (model.Block, error) {
	block, ok := s.records[number]
	if !ok {
		return model.Block{}, store.ErrBlockNotFound
	}
	return block, nil
}
func (s *repository) CanonicalLogs(_ context.Context, _ uint64, from, to uint64) ([]model.Log, error) {
	var logs []model.Log
	for n := from; n <= to; n++ {
		logs = append(logs, s.logs[n]...)
	}
	return logs, nil
}
func newRunner(n uint64) *Runner {
	blocks := make([]model.Block, n+1)
	for i := uint64(0); i <= n; i++ {
		parent := h(byte(i))
		if i == 0 {
			parent = common.Hash{}
		}
		blocks[i] = model.Block{Number: i, Hash: h(byte(i + 1)), ParentHash: parent, Time: time.Unix(int64(i+1), 0).UTC()}
	}
	return &Runner{Source: &source{chain: 31337, head: n, blocks: blocks}, Store: &repository{}, ChainID: 31337, Genesis: blocks[0].Hash, StartBlock: 1, Filter: rpc.Filter{Addresses: []common.Address{{19: 1}}}, Config: Config{InitialRange: 4, MinRange: 1, MaxRange: 8, MaxAttempts: 4, RangeTimeout: time.Second, HealthyMaxLogs: 3, HealthyLatency: time.Second, GrowthAfter: 2, MaxLogsPerRange: 100}}
}
func TestNormalBackfillAndRestartFrontier(t *testing.T) {
	r := newRunner(6)
	res, err := r.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Checkpoint.Number != 6 || !reflect.DeepEqual(r.Store.(*repository).calls, [][2]uint64{{1, 4}, {5, 6}}) {
		t.Fatalf("coverage: %+v %#v", res, r.Store.(*repository).calls)
	}
	res, err = r.Run(context.Background())
	if err != nil || res.Ranges != 0 || res.Checkpoint.Number != 6 {
		t.Fatalf("restart: %+v %v", res, err)
	}
}
func TestShrinkRetriesSameFromAndGrowth(t *testing.T) {
	r := newRunner(8)
	r.Config.InitialRange = 4
	r.Config.GrowthAfter = 1
	s := r.Source.(*source)
	limited := false
	s.logs = func(_ context.Context, from, to uint64) ([]model.Log, error) {
		if !limited && from == 1 && to == 4 {
			limited = true
			return nil, &rpc.RPCError{Code: -32005, Message: "too many results"}
		}
		return nil, nil
	}
	var requested [][2]uint64
	r.OnRange = func(from, to uint64, _ int, _ uint64) { requested = append(requested, [2]uint64{from, to}) }
	res, err := r.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := [][2]uint64{{1, 4}, {1, 2}, {3, 6}, {7, 8}}
	if !reflect.DeepEqual(requested, want) || res.Checkpoint.Number != 8 || res.Retries != 1 {
		t.Fatalf("ranges %v, result %+v", requested, res)
	}
}
func TestAttemptExhaustionAndMinimumLimit(t *testing.T) {
	r := newRunner(3)
	r.Config.MaxAttempts = 2
	r.Config.InitialRange = 1
	r.Source.(*source).logs = func(context.Context, uint64, uint64) ([]model.Log, error) {
		return nil, errors.New("temporary transport failure")
	}
	_, err := r.Run(context.Background())
	if !errors.Is(err, ErrAttempts) {
		t.Fatalf("attempts: %v", err)
	}
	r.Source.(*source).logs = func(context.Context, uint64, uint64) ([]model.Log, error) {
		return nil, &rpc.RPCError{Code: -32005, Message: "too many results"}
	}
	_, err = r.Run(context.Background())
	if !errors.Is(err, ErrRangeLimit) {
		t.Fatalf("min limit: %v", err)
	}
}
func TestCancellationAndWrongChain(t *testing.T) {
	r := newRunner(2)
	r.Source.(*source).chain = 1
	_, err := r.Run(context.Background())
	if !errors.Is(err, ErrWrongChain) {
		t.Fatalf("wrong chain: %v", err)
	}
	r.Source.(*source).chain = 31337
	r.Source.(*source).logs = func(ctx context.Context, _, _ uint64) ([]model.Log, error) { <-ctx.Done(); return nil, ctx.Err() }
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = r.Run(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > time.Second {
		t.Fatalf("cancellation: %v", err)
	}
}
func TestNonzeroStartRequiresTrustedAnchor(t *testing.T) {
	r := newRunner(5)
	r.StartBlock = 3
	if _, err := r.Run(context.Background()); err == nil {
		t.Fatal("accepted unpinned anchor")
	}
	r.AnchorHash = h(99)
	if _, err := r.Run(context.Background()); !errors.Is(err, ErrWrongChain) {
		t.Fatalf("wrong anchor: %v", err)
	}
	r.AnchorHash = r.Source.(*source).blocks[2].Hash
	res, err := r.Run(context.Background())
	if err != nil || res.Checkpoint.Number != 5 {
		t.Fatalf("pinned anchor: %+v %v", res, err)
	}
}
