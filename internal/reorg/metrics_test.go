package reorg

import (
	"bytes"
	"strings"
	"testing"

	"reorgguard/internal/store"
)

func TestMetricsFixedSeriesAndReasonVocabulary(t *testing.T) {
	m := NewMetrics()
	m.Success(store.ReorgResult{Ancestor: store.Checkpoint{Number: 3}, Depth: 2, OrphanedBlocks: 2, OrphanedLogs: 1})
	m.Failure("depth_exceeded")
	m.Failure("rpc_secret_label")
	var b bytes.Buffer
	if err := m.WritePrometheus(&b); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{"reorgguard_reorgs_total 1", "reorgguard_reorg_depth_blocks 2", "reorgguard_orphaned_blocks_total 2", "reorgguard_orphaned_logs_total 1", "reorgguard_common_ancestor_height 3", "reorgguard_reconciliation_failures_total{reason=\"depth_exceeded\"} 1", "reorgguard_reconciliation_failures_total{reason=\"other\"} 1"} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in %s", want, out)
		}
	}
	if strings.Contains(out, "rpc_secret_label") {
		t.Fatal("unbounded metric label")
	}
}
