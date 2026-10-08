// Package reorg proves a common ancestor before changing canonical storage.
package reorg

import (
	"context"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"reorgguard/internal/model"
)

var ErrDepth = errors.New("reorg exceeds maximum depth")
var ErrAncestorMissing = errors.New("common ancestor missing from retained canonical history")
var ErrParentChain = errors.New("remote parent chain is invalid")
var ErrNoFork = errors.New("candidate already extends current canonical chain")
var ErrRemoteLookup = errors.New("remote parent lookup failed")

type LocalBlock func(context.Context, uint64) (model.Block, error)
type RemoteParent func(context.Context, common.Hash) (model.Block, error)

// FindAncestor walks remote parent hashes, comparing them with retained local
// canonical blocks at the same height. Heights only bound the search; hash and
// parent continuity prove the ancestor. The replacement is returned in order.
func FindAncestor(ctx context.Context, oldHead, candidate model.Block, maxDepth uint64, local LocalBlock, remote RemoteParent) (model.Block, []model.Block, error) {
	if maxDepth == 0 || candidate.Number > oldHead.Number || candidate.Hash == (common.Hash{}) || oldHead.Hash == (common.Hash{}) || local == nil || remote == nil {
		return model.Block{}, nil, model.ErrInvalidRange
	}
	current := candidate
	var reverse []model.Block
	seen := map[common.Hash]bool{}
	for {
		if err := ctx.Err(); err != nil {
			return model.Block{}, nil, err
		}
		if oldHead.Number-current.Number > maxDepth {
			return model.Block{}, nil, ErrDepth
		}
		if seen[current.Hash] {
			return model.Block{}, nil, ErrParentChain
		}
		seen[current.Hash] = true
		stored, err := local(ctx, current.Number)
		if err != nil {
			if ctx.Err() != nil {
				return model.Block{}, nil, ctx.Err()
			}
			return model.Block{}, nil, fmt.Errorf("height %d: %w", current.Number, err)
		}
		if stored.Number != current.Number || stored.Hash == (common.Hash{}) {
			return model.Block{}, nil, ErrAncestorMissing
		}
		if stored.Hash == current.Hash {
			if len(reverse) == 0 {
				return model.Block{}, nil, ErrNoFork
			}
			forward := make([]model.Block, len(reverse))
			for i := range reverse {
				forward[i] = reverse[len(reverse)-1-i]
			}
			return current, forward, nil
		}
		if current.Number == 0 {
			return model.Block{}, nil, ErrAncestorMissing
		}
		reverse = append(reverse, current)
		parent, err := remote(ctx, current.ParentHash)
		if err != nil {
			if ctx.Err() != nil {
				return model.Block{}, nil, ctx.Err()
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return model.Block{}, nil, err
			}
			return model.Block{}, nil, fmt.Errorf("parent of height %d: %w: %v", current.Number, ErrRemoteLookup, err)
		}
		if parent.Hash != current.ParentHash || parent.Number+1 != current.Number || parent.Hash == (common.Hash{}) {
			return model.Block{}, nil, ErrParentChain
		}
		current = parent
	}
}
