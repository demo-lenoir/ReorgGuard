package reorg

import (
	"fmt"
	"io"
	"sync"

	"reorgguard/internal/store"
)

// Metrics uses fixed series and a finite failure-reason vocabulary. The text
// exposition can be mounted by the later operational API without dynamic labels.
type Metrics struct {
	mu             sync.Mutex
	reorgs         uint64
	depth          uint64
	orphanedBlocks uint64
	orphanedLogs   uint64
	ancestorHeight uint64
	failures       map[string]uint64
}

func NewMetrics() *Metrics { return &Metrics{failures: make(map[string]uint64)} }
func (m *Metrics) Success(r store.ReorgResult) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reorgs++
	m.depth = r.Depth
	m.orphanedBlocks += r.OrphanedBlocks
	m.orphanedLogs += r.OrphanedLogs
	m.ancestorHeight = r.Ancestor.Number
}
func (m *Metrics) Failure(reason string) {
	if m == nil {
		return
	}
	if !validReason(reason) {
		reason = "other"
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failures[reason]++
}
func validReason(reason string) bool {
	switch reason {
	case "depth_exceeded", "ancestor_missing", "parent_chain_invalid", "identity_conflict", "provider_inconsistent", "database_failure", "interrupted", "other":
		return true
	}
	return false
}
func (m *Metrics) WritePrometheus(w io.Writer) error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	series := []struct {
		name, kind string
		value      uint64
	}{
		{"reorgguard_reorgs_total", "counter", m.reorgs},
		{"reorgguard_reorg_depth_blocks", "gauge", m.depth},
		{"reorgguard_orphaned_blocks_total", "counter", m.orphanedBlocks},
		{"reorgguard_orphaned_logs_total", "counter", m.orphanedLogs},
		{"reorgguard_common_ancestor_height", "gauge", m.ancestorHeight},
	}
	for _, s := range series {
		if _, err := fmt.Fprintf(w, "# TYPE %s %s\n%s %d\n", s.name, s.kind, s.name, s.value); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(w, "# TYPE reorgguard_reconciliation_failures_total counter"); err != nil {
		return err
	}
	for _, reason := range []string{"depth_exceeded", "ancestor_missing", "parent_chain_invalid", "identity_conflict", "provider_inconsistent", "database_failure", "interrupted", "other"} {
		if _, err := fmt.Fprintf(w, "reorgguard_reconciliation_failures_total{reason=%q} %d\n", reason, m.failures[reason]); err != nil {
			return err
		}
	}
	return nil
}
