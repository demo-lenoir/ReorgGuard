package reorg

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"reorgguard/internal/model"
)

func testHash(x byte) common.Hash { var h common.Hash; h[31] = x; return h }
func testBlock(n uint64, hash, parent byte) model.Block {
	return model.Block{Number: n, Hash: testHash(hash), ParentHash: testHash(parent), Time: time.Unix(int64(n+1), 0).UTC()}
}
func TestFindAncestorDepthAndParentProof(t *testing.T) {
	localBlocks := map[uint64]model.Block{1: testBlock(1, 1, 9), 2: testBlock(2, 2, 1), 3: testBlock(3, 3, 2), 4: testBlock(4, 4, 3)}
	remoteBlocks := map[common.Hash]model.Block{testHash(30): testBlock(3, 30, 2), testHash(40): testBlock(4, 40, 30), testHash(2): localBlocks[2]}
	local := func(_ context.Context, n uint64) (model.Block, error) {
		b, ok := localBlocks[n]
		if !ok {
			return model.Block{}, ErrAncestorMissing
		}
		return b, nil
	}
	remote := func(_ context.Context, h common.Hash) (model.Block, error) {
		b, ok := remoteBlocks[h]
		if !ok {
			return model.Block{}, errors.New("missing")
		}
		return b, nil
	}
	ancestor, branch, err := FindAncestor(context.Background(), localBlocks[4], remoteBlocks[testHash(40)], 2, local, remote)
	if err != nil || ancestor.Hash != testHash(2) || len(branch) != 2 || branch[0].Hash != testHash(30) || branch[1].Hash != testHash(40) {
		t.Fatalf("ancestor=%+v branch=%+v err=%v", ancestor, branch, err)
	}
	if _, _, err = FindAncestor(context.Background(), localBlocks[4], remoteBlocks[testHash(40)], 1, local, remote); !errors.Is(err, ErrDepth) {
		t.Fatalf("depth: %v", err)
	}
	delete(localBlocks, 2)
	if _, _, err = FindAncestor(context.Background(), localBlocks[4], remoteBlocks[testHash(40)], 2, local, remote); !errors.Is(err, ErrAncestorMissing) {
		t.Fatalf("missing ancestor: %v", err)
	}
}
func TestFindAncestorRejectsPlausibleHeightsWithBadParents(t *testing.T) {
	localBlocks := map[uint64]model.Block{1: testBlock(1, 1, 9), 2: testBlock(2, 2, 1), 3: testBlock(3, 3, 2)}
	local := func(_ context.Context, n uint64) (model.Block, error) { return localBlocks[n], nil }
	bad := testBlock(3, 30, 2)
	remote := func(_ context.Context, _ common.Hash) (model.Block, error) { return testBlock(2, 22, 1), nil } // plausible height, wrong hash
	if _, _, err := FindAncestor(context.Background(), localBlocks[3], bad, 2, local, remote); !errors.Is(err, ErrParentChain) {
		t.Fatalf("accepted fake parent: %v", err)
	}
	remote = func(_ context.Context, _ common.Hash) (model.Block, error) { return testBlock(3, 2, 1), nil } // matching hash byte, wrong height
	if _, _, err := FindAncestor(context.Background(), localBlocks[3], bad, 2, local, remote); !errors.Is(err, ErrParentChain) {
		t.Fatalf("accepted fake height: %v", err)
	}
}
func TestFindAncestorCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	local := func(_ context.Context, n uint64) (model.Block, error) { return testBlock(n, byte(n), byte(n-1)), nil }
	remote := func(ctx context.Context, _ common.Hash) (model.Block, error) {
		cancel()
		return model.Block{}, ctx.Err()
	}
	_, _, err := FindAncestor(ctx, testBlock(3, 3, 2), testBlock(3, 30, 2), 2, local, remote)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
}
func FuzzFindAncestor(f *testing.F) {
	f.Add(byte(2), byte(2))
	f.Add(byte(4), byte(3))
	f.Fuzz(func(t *testing.T, old, depth byte) {
		if old < 2 {
			old = 2
		}
		max := uint64(depth%8 + 1)
		local := func(_ context.Context, n uint64) (model.Block, error) { return testBlock(n, byte(n), byte(n-1)), nil }
		remote := func(_ context.Context, h common.Hash) (model.Block, error) {
			return testBlock(uint64(h[31]), h[31], h[31]-1), nil
		}
		_, _, _ = FindAncestor(context.Background(), testBlock(uint64(old), old, old-1), testBlock(uint64(old), old+1, old), max, local, remote)
	})
}
