package reorg

import (
	"context"
	"errors"

	"reorgguard/internal/backfill"
	"reorgguard/internal/model"
	"reorgguard/internal/store"
)

// Coordinator uses the same append path. A detected fork is resolved
// once against the same observed target, then the uncovered suffix is fetched
// again from the newly durable checkpoint.
type Coordinator struct {
	Backfill *backfill.Runner
	Engine   *Engine
}

func (c *Coordinator) Run(ctx context.Context) (backfill.Result, error) {
	if c == nil || c.Backfill == nil || c.Engine == nil {
		return backfill.Result{}, errors.New("invalid coordinator configuration")
	}
	target, err := c.Backfill.Source.BlockNumber(ctx)
	if err != nil {
		return backfill.Result{}, err
	}
	return c.RunTo(ctx, target)
}

// RunTo uses the same append/reconciliation path for one observed HTTP head.
func (c *Coordinator) RunTo(ctx context.Context, target uint64) (backfill.Result, error) {
	if c == nil || c.Backfill == nil || c.Engine == nil {
		return backfill.Result{}, errors.New("invalid coordinator configuration")
	}
	for attempt := 0; attempt < 3; attempt++ {
		result, err := c.Backfill.RunTo(ctx, target)
		if err == nil {
			return result, nil
		}
		if errors.Is(err, backfill.ErrCheckpointMoved) {
			continue
		}
		if !errors.Is(err, store.ErrReorgRequired) && !errors.Is(err, model.ErrParent) {
			return result, err
		}
		if _, err = c.Engine.Reconcile(ctx, target); err != nil {
			if errors.Is(err, ErrCandidateMoved) {
				continue
			}
			return result, err
		}
	}
	return backfill.Result{}, ErrCandidateMoved
}
