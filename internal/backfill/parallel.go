package backfill

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"reorgguard/internal/crashprobe"
	"reorgguard/internal/model"
	"reorgguard/internal/rpc"
	"reorgguard/internal/store"
	"reorgguard/internal/telemetry"
)

// ParallelStats is a bounded, lock-free snapshot of fetch concurrency. Values
// are gauges/counters only; endpoint IDs and URLs are never recorded here.
type ParallelStats struct {
	active       atomic.Int64
	maxActive    atomic.Int64
	inFlight     atomic.Int64
	maxInFlight  atomic.Int64
	pending      atomic.Int64
	maxPending   atomic.Int64
	backpressure atomic.Uint64
}
type ParallelSnapshot struct {
	Active, MaxActive, InFlight, MaxInFlight, Pending, MaxPending int64
	Backpressure                                                  uint64
}

func (s *ParallelStats) Snapshot() ParallelSnapshot {
	if s == nil {
		return ParallelSnapshot{}
	}
	return ParallelSnapshot{s.active.Load(), s.maxActive.Load(), s.inFlight.Load(), s.maxInFlight.Load(), s.pending.Load(), s.maxPending.Load(), s.backpressure.Load()}
}
func high(w *atomic.Int64, n int64) {
	for {
		old := w.Load()
		if n <= old || w.CompareAndSwap(old, n) {
			return
		}
	}
}
func (s *ParallelStats) start() {
	if s == nil {
		return
	}
	high(&s.maxActive, s.active.Add(1))
}
func (s *ParallelStats) stop() {
	if s != nil {
		s.active.Add(-1)
	}
}
func (s *ParallelStats) queued() {
	if s == nil {
		return
	}
	high(&s.maxInFlight, s.inFlight.Add(1))
}
func (s *ParallelStats) done() {
	if s != nil {
		s.inFlight.Add(-1)
	}
}
func (s *ParallelStats) buffered() {
	if s == nil {
		return
	}
	high(&s.maxPending, s.pending.Add(1))
}
func (s *ParallelStats) consumed() {
	if s != nil {
		s.pending.Add(-1)
	}
}
func (s *ParallelStats) blocked() {
	if s != nil {
		s.backpressure.Add(1)
	}
}

type rangeTask struct {
	from, to uint64
	attempt  int
	window   uint64
	deadline time.Time
}
type rangeResult struct {
	task    rangeTask
	items   []model.BlockData
	logs    int
	elapsed time.Duration
	err     error
}

func (r *Runner) fetchTask(ctx context.Context, task rangeTask) rangeResult {
	result := rangeResult{task: task}
	started := time.Now()
	logs, err := r.Source.Logs(ctx, task.from, task.to, r.Filter)
	if err == nil {
		err = r.Filter.ValidateLogs(logs)
	}
	if err == nil && len(logs) > r.Config.MaxLogsPerRange {
		err = rpc.ErrResponseTooLarge
	}
	if err == nil {
		blocks := make([]model.Block, 0, task.to-task.from+1)
		for n := task.from; n <= task.to; n++ {
			b, blockErr := r.Source.BlockByNumber(ctx, n)
			if blockErr != nil {
				err = blockErr
				break
			}
			blocks = append(blocks, b)
		}
		if err == nil {
			err = crashprobe.Pause(ctx, "after_range_fetch")
		}
		if err == nil {
			// Intra-range linkage can be validated before the earlier range
			// finishes. Its first parent is checked against the ordered frontier
			// immediately before the SQL append.
			_, validationSpan := telemetry.Start(ctx, "backfill.validate_range", attribute.Int("block.count", len(blocks)))
			items, buildErr := model.BuildRange(task.from, task.to, blocks[0].ParentHash, blocks, logs)
			if buildErr != nil {
				telemetry.Failure(validationSpan, "range_invalid")
			}
			validationSpan.End()
			result.items = items
			err = buildErr
		}
	}
	result.logs = len(logs)
	result.elapsed = time.Since(started)
	result.err = err
	return result
}

func (r *Runner) runParallel(ctx context.Context, target uint64, checkpoint store.Checkpoint, result Result, window *uint64, healthy *int) (Result, error) {
	from := checkpoint.Number + 1
	attempts := make(map[uint64]int)
	deadlines := make(map[uint64]time.Time)
	for from <= target {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		waveWindow := *window
		tasks := make([]rangeTask, 0, r.Config.MaxInFlight)
		for next := from; next <= target && len(tasks) < r.Config.MaxInFlight; {
			to := next + waveWindow - 1
			if to < next || to > target {
				to = target
			}
			tasks = append(tasks, rangeTask{from: next, to: to, attempt: attempts[next] + 1, window: waveWindow})
			if to == target {
				break
			}
			next = to + 1
		}
		waveCtx, cancel := context.WithCancel(ctx)
		results := make(chan rangeResult, len(tasks))
		workerSlots := make(chan struct{}, r.Config.Workers)
		var wg sync.WaitGroup
		for _, task := range tasks {
			if r.OnRange != nil {
				r.OnRange(task.from, task.to, task.attempt, task.window)
			}
			if deadlines[task.from].IsZero() {
				deadlines[task.from] = time.Now().Add(r.Config.RangeTimeout)
			}
			task.deadline = deadlines[task.from]
			r.Parallel.queued()
			wg.Add(1)
			go func(task rangeTask) {
				defer wg.Done()
				defer r.Parallel.done()
				select {
				case workerSlots <- struct{}{}:
				case <-waveCtx.Done():
					return
				}
				defer func() { <-workerSlots }()
				r.Parallel.start()
				defer r.Parallel.stop()
				work, stop := context.WithDeadline(waveCtx, task.deadline)
				defer stop()
				fetched := r.fetchTask(work, task)
				r.Parallel.buffered()
				select {
				case results <- fetched:
				case <-waveCtx.Done():
					r.Parallel.consumed()
				}
			}(task)
		}
		// The result channel and map each contain at most MaxInFlight entries.
		pending := make(map[uint64]rangeResult, len(tasks))
		var failed error
		var failedTask rangeTask
		for _, task := range tasks {
			var ready rangeResult
			for {
				if value, ok := pending[task.from]; ok {
					ready = value
					delete(pending, task.from)
					break
				}
				if len(pending) > 0 {
					r.Parallel.blocked()
				}
				select {
				case <-ctx.Done():
					failed = ctx.Err()
				case value := <-results:
					pending[value.task.from] = value
					if value.task.from != task.from {
						if pauseErr := crashprobe.Pause(ctx, "parallel_out_of_order_buffered"); pauseErr != nil {
							failed = pauseErr
						}
					}
				}
				if failed != nil {
					break
				}
			}
			if failed != nil {
				break
			}
			r.Parallel.consumed()
			if ready.err != nil {
				failed = ready.err
				failedTask = task
				break
			}
			if len(ready.items) == 0 || ready.items[0].Block.ParentHash != checkpoint.Hash {
				failed = model.ErrParent
				failedTask = task
				break
			}
			if err := crashprobe.Pause(ctx, "parallel_ordered_result_wait"); err != nil {
				failed = err
				failedTask = task
				break
			}
			if err := r.Store.Append(ctx, r.ChainID, ready.items); err != nil {
				failed = err
				failedTask = task
				break
			}
			checkpoint = store.Checkpoint{Number: task.to, Hash: ready.items[len(ready.items)-1].Block.Hash}
			result.Checkpoint = checkpoint
			result.Ranges++
			result.Logs += ready.logs
			r.Metrics.observe(task.from, task.to, ready.logs, ready.items)
			from = task.to + 1
			delete(attempts, task.from)
			delete(deadlines, task.from)
			if ready.logs <= r.Config.HealthyMaxLogs && ready.elapsed <= r.Config.HealthyLatency {
				*healthy++
			} else {
				*healthy = 0
			}
			if *healthy >= r.Config.GrowthAfter && *window < r.Config.MaxRange {
				if *window > r.Config.MaxRange/2 {
					*window = r.Config.MaxRange
				} else {
					*window *= 2
				}
				*healthy = 0
			}
		}
		cancel()
		wg.Wait()
		// Results not consumed because a range failed were never committed.
		for range pending {
			r.Parallel.consumed()
		}
		for len(results) > 0 {
			<-results
			r.Parallel.consumed()
		}
		if failed == nil {
			continue
		}
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		if errors.Is(failed, model.ErrParent) || errors.Is(failed, model.ErrIdentity) || errors.Is(failed, model.ErrInvalidRange) {
			return result, failed
		}
		if failedTask.from == 0 && from != 0 {
			return result, failed
		}
		attempts[failedTask.from]++
		result.Retries++
		if rpc.IsRangeLimit(failed) {
			if *window == r.Config.MinRange {
				return result, fmt.Errorf("%w: %w", ErrRangeLimit, failed)
			}
			*window /= 2
			if *window < r.Config.MinRange {
				*window = r.Config.MinRange
			}
			*healthy = 0
		}
		if attempts[failedTask.from] >= r.Config.MaxAttempts || time.Now().After(deadlines[failedTask.from]) {
			return result, fmt.Errorf("%w: %w", ErrAttempts, failed)
		}
		if r.Config.RetryDelay > 0 {
			timer := time.NewTimer(r.Config.RetryDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return result, ctx.Err()
			case <-timer.C:
			}
		}
		from = failedTask.from
	}
	return result, nil
}
