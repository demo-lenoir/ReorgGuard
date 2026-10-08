package backfill

import (
	"fmt"
	"io"
	"sync/atomic"

	"reorgguard/internal/model"
)

// Metrics counts successfully committed range observations. Counters reset on
// process restart, as Prometheus process counters normally do.
type Metrics struct {
	rangeBlocks atomic.Uint64
	logs        atomic.Uint64
	duplicates  atomic.Uint64
}

func (m *Metrics) observe(from, to uint64, raw int, items []model.BlockData) {
	if m == nil {
		return
	}
	unique := 0
	for _, item := range items {
		unique += len(item.Logs)
	}
	m.rangeBlocks.Store(to - from + 1)
	m.logs.Add(uint64(raw))
	if raw > unique {
		m.duplicates.Add(uint64(raw - unique))
	}
}
func (m *Metrics) WritePrometheus(w io.Writer) error {
	if m == nil {
		return nil
	}
	_, err := fmt.Fprintf(w, "reorgguard_backfill_range_blocks %d\nreorgguard_logs_processed_total %d\nreorgguard_duplicate_logs_total %d\n", m.rangeBlocks.Load(), m.logs.Load(), m.duplicates.Load())
	return err
}
