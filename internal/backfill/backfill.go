// Package backfill drives one ordered historical HTTP-to-PostgreSQL append path.
package backfill

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"go.opentelemetry.io/otel/attribute"
	"reorgguard/internal/crashprobe"
	"reorgguard/internal/model"
	"reorgguard/internal/rpc"
	"reorgguard/internal/store"
	"reorgguard/internal/telemetry"
)

var ErrAttempts = errors.New("range attempt limit exhausted")
var ErrWrongChain = errors.New("RPC chain ID or genesis mismatch")
var ErrRangeLimit = errors.New("provider range limit at minimum window")

type Source interface {
	ChainID(context.Context) (uint64, error)
	BlockNumber(context.Context) (uint64, error)
	CheckpointSource
}
type Repository interface {
	Init(context.Context, store.Dataset) (store.Checkpoint, error)
	Append(context.Context, uint64, []model.BlockData) error
	CheckpointRepository
}
type Config struct {
	InitialRange    uint64
	MinRange        uint64
	MaxRange        uint64
	MaxAttempts     int
	RangeTimeout    time.Duration
	RetryDelay      time.Duration
	HealthyMaxLogs  int
	HealthyLatency  time.Duration
	GrowthAfter     int
	MaxLogsPerRange int
	Workers         int // zero/one uses serial fetching
	MaxInFlight     int
}

func (c Config) Validate() error {
	if c.MinRange == 0 || c.MinRange > c.InitialRange || c.InitialRange > c.MaxRange || c.MaxRange > 10000 || c.MaxAttempts < 1 || c.MaxAttempts > 100 || c.RangeTimeout <= 0 || c.RetryDelay < 0 || c.HealthyMaxLogs < 0 || c.HealthyLatency <= 0 || c.GrowthAfter < 1 || c.MaxLogsPerRange < 1 || c.Workers < 0 || c.Workers > 32 || c.MaxInFlight < 0 || c.MaxInFlight > 64 || (c.Workers > 1 && (c.MaxInFlight < c.Workers || c.MaxInFlight == 0)) {
		return errors.New("invalid backfill configuration")
	}
	return nil
}

type Runner struct {
	Source     Source
	Store      Repository
	Config     Config
	ChainID    uint64
	Genesis    common.Hash
	AnchorHash common.Hash // required when StartBlock > 1
	StartBlock uint64
	Filter     rpc.Filter
	// OnRange is a bounded-observer hook for tests/metrics. It is called synchronously.
	OnRange  func(from, to uint64, attempt int, window uint64)
	Adaptive *AdaptiveState // optional per-endpoint capability memory
	Parallel *ParallelStats // optional bounded-worker metrics
	Metrics  *Metrics
}
type AdaptiveState struct {
	mu      sync.Mutex
	window  uint64
	healthy int
}

func (a *AdaptiveState) load(initial uint64) (uint64, int) {
	if a == nil {
		return initial, 0
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.window == 0 {
		return initial, 0
	}
	return a.window, a.healthy
}
func (a *AdaptiveState) save(window uint64, healthy int) {
	if a == nil {
		return
	}
	a.mu.Lock()
	a.window = window
	a.healthy = healthy
	a.mu.Unlock()
}
func (a *AdaptiveState) Snapshot() (uint64, int) {
	if a == nil {
		return 0, 0
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.window, a.healthy
}

type Result struct {
	Checkpoint store.Checkpoint
	Ranges     int
	Logs       int
	Retries    int
}

func (r *Runner) Run(ctx context.Context) (Result, error) {
	if err := r.validate(); err != nil {
		return Result{}, err
	}
	head, err := r.Source.BlockNumber(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("read remote head: %w", err)
	}
	return r.RunTo(ctx, head)
}
func (r *Runner) validate() error {
	if r.Source == nil || r.Store == nil || r.ChainID > math.MaxInt64 || r.StartBlock > math.MaxInt64 || r.Genesis == (common.Hash{}) || (r.StartBlock > 1 && r.AnchorHash == (common.Hash{})) {
		return errors.New("invalid runner configuration")
	}
	if err := r.Config.Validate(); err != nil {
		return err
	}
	return r.Filter.Validate()
}

// RunTo is deterministic for a fixed target and source fixture. It still checks
// network identity and the durable checkpoint before fetching uncovered ranges.
func (r *Runner) RunTo(ctx context.Context, target uint64) (Result, error) {
	ctx, span := telemetry.Start(ctx, "backfill.run", attribute.Int64("target.block", int64(target)))
	defer span.End()
	if err := r.validate(); err != nil {
		return Result{}, err
	}
	if target > math.MaxInt64 {
		return Result{}, model.ErrInvalidRange
	}
	chainID, err := r.Source.ChainID(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("read chain ID: %w", err)
	}
	if chainID != r.ChainID {
		return Result{}, ErrWrongChain
	}
	genesis, err := r.Source.BlockByNumber(ctx, 0)
	if err != nil {
		return Result{}, fmt.Errorf("read genesis: %w", err)
	}
	if genesis.Hash != r.Genesis {
		return Result{}, ErrWrongChain
	}
	anchorNumber := uint64(0)
	if r.StartBlock > 0 {
		anchorNumber = r.StartBlock - 1
	}
	anchor := genesis
	if anchorNumber > 0 {
		anchor, err = r.Source.BlockByNumber(ctx, anchorNumber)
		if err != nil {
			return Result{}, fmt.Errorf("read anchor: %w", err)
		}
	}
	if r.StartBlock > 1 && anchor.Hash != r.AnchorHash {
		return Result{}, ErrWrongChain
	}
	checkpoint, err := r.Store.Init(ctx, store.Dataset{ChainID: r.ChainID, Genesis: r.Genesis, FilterHash: r.Filter.Fingerprint(), StartBlock: r.StartBlock, Anchor: anchor})
	if err != nil {
		return Result{}, fmt.Errorf("initialize dataset: %w", err)
	}
	fork, err := VerifyCheckpoint(ctx, r.Source, r.Store, r.ChainID, checkpoint, r.StartBlock, r.Filter)
	if err != nil {
		return Result{}, fmt.Errorf("verify checkpoint: %w", err)
	}
	if fork {
		return Result{}, store.ErrReorgRequired
	}
	result := Result{Checkpoint: checkpoint}
	if target <= checkpoint.Number {
		return result, nil
	}
	window, healthy := r.Adaptive.load(r.Config.InitialRange)
	defer func() { r.Adaptive.save(window, healthy) }()
	if r.Config.Workers > 1 {
		return r.runParallel(ctx, target, checkpoint, result, &window, &healthy)
	}
	for from := checkpoint.Number + 1; from <= target; {
		if err := crashprobe.Pause(ctx, "before_range_fetch"); err != nil {
			return result, err
		}
		rangeCtx, cancel := context.WithTimeout(ctx, r.Config.RangeTimeout)
		var items []model.BlockData
		var to uint64
		var logCount int
		var elapsed time.Duration
		var rangeErr error
		for attempt := 1; attempt <= r.Config.MaxAttempts; attempt++ {
			if err = rangeCtx.Err(); err != nil {
				rangeErr = err
				break
			}
			to = from + window - 1
			if to < from || to > target {
				to = target
			}
			if r.OnRange != nil {
				r.OnRange(from, to, attempt, window)
			}
			started := time.Now()
			var fetched []model.Log
			fetched, rangeErr = r.Source.Logs(rangeCtx, from, to, r.Filter)
			if rangeErr == nil {
				rangeErr = r.Filter.ValidateLogs(fetched)
			}
			if rangeErr == nil && len(fetched) > r.Config.MaxLogsPerRange {
				rangeErr = rpc.ErrResponseTooLarge
			}
			if rangeErr == nil {
				blocks := make([]model.Block, 0, to-from+1)
				for n := from; n <= to; n++ {
					var b model.Block
					b, rangeErr = r.Source.BlockByNumber(rangeCtx, n)
					if rangeErr != nil {
						break
					}
					blocks = append(blocks, b)
				}
				if rangeErr == nil {
					rangeErr = crashprobe.Pause(rangeCtx, "after_range_fetch")
				}
				if rangeErr == nil {
					_, validationSpan := telemetry.Start(rangeCtx, "backfill.validate_range", attribute.Int("block.count", len(blocks)))
					items, rangeErr = model.BuildRange(from, to, checkpoint.Hash, blocks, fetched)
					if rangeErr != nil {
						telemetry.Failure(validationSpan, "range_invalid")
					}
					validationSpan.End()
				}
			}
			elapsed = time.Since(started)
			if rangeErr == nil {
				logCount = len(fetched)
				break
			}
			result.Retries++
			if rpc.IsRangeLimit(rangeErr) {
				if window == r.Config.MinRange {
					rangeErr = fmt.Errorf("%w: %w", ErrRangeLimit, rangeErr)
					break
				}
				window /= 2
				if window < r.Config.MinRange {
					window = r.Config.MinRange
				}
				healthy = 0
			} else if errors.Is(rangeErr, model.ErrParent) || errors.Is(rangeErr, model.ErrIdentity) || errors.Is(rangeErr, model.ErrInvalidRange) {
				break
			}
			if attempt == r.Config.MaxAttempts {
				rangeErr = fmt.Errorf("%w: %w", ErrAttempts, rangeErr)
				break
			}
			if r.Config.RetryDelay > 0 {
				timer := time.NewTimer(r.Config.RetryDelay)
				select {
				case <-rangeCtx.Done():
					timer.Stop()
					rangeErr = rangeCtx.Err()
				case <-timer.C:
				}
				if rangeCtx.Err() != nil {
					break
				}
			}
		}
		cancel()
		if rangeErr != nil {
			return result, fmt.Errorf("fetch range from %d: %w", from, rangeErr)
		}
		if len(items) == 0 {
			return result, ErrAttempts
		}
		if err = crashprobe.Pause(ctx, "after_range_validated"); err != nil {
			return result, err
		}
		if err = r.Store.Append(ctx, r.ChainID, items); err != nil {
			return result, fmt.Errorf("commit range %d-%d: %w", from, to, err)
		}
		checkpoint = store.Checkpoint{Number: to, Hash: items[len(items)-1].Block.Hash}
		result.Checkpoint = checkpoint
		result.Ranges++
		result.Logs += logCount
		r.Metrics.observe(from, to, logCount, items)
		if err = crashprobe.Pause(ctx, "between_ranges"); err != nil {
			return result, err
		}
		if logCount <= r.Config.HealthyMaxLogs && elapsed <= r.Config.HealthyLatency {
			healthy++
		} else {
			healthy = 0
		}
		if healthy >= r.Config.GrowthAfter && window < r.Config.MaxRange {
			if window > r.Config.MaxRange/2 {
				window = r.Config.MaxRange
			} else {
				window *= 2
			}
			healthy = 0
		}
		from = to + 1
	}
	return result, nil
}
