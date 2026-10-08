//go:build crash

package crashharness

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	"reorgguard/internal/reorg"
	"reorgguard/internal/store"
)

type crashCase struct {
	name      string
	boundary  string
	point     string
	hold      string
	pre       []string
	final     string
	head      uint64
	empty     bool
	duplicate bool
}

func exerciseCrash(t *testing.T, tc crashCase, reference *snapshot) {
	t.Helper()
	f := newRPCFixture(t, tc.empty, tc.duplicate)
	var ref snapshot
	if reference == nil {
		stages := append(append([]string(nil), tc.pre...), tc.final)
		ref = referenceRun(t, f, stages, tc.head)
	} else {
		ref = *reference
	}
	dsn := schemaDSN(t)
	for _, stage := range tc.pre {
		f.setBranch(stage, tc.head)
		runSuccess(t, dsn, f)
	}
	f.setBranch(tc.final, tc.head)
	var control *probeController
	if tc.point != "" {
		control = newController(t)
	}
	if tc.hold != "" {
		f.holdAt(tc.hold)
	}
	p := startChild(t, dsn, f, control, tc.point)
	if tc.point != "" {
		ev := waitProbe(t, control, tc.point)
		defer ev.conn.Close()
	} else {
		waitRPC(t, f, tc.hold)
	}
	pre := inspect(t, dsn)
	p.kill(t)
	if tc.hold != "" {
		f.releaseHold()
	}
	crashed := inspect(t, dsn)
	compare(t, crashed, pre) // no half-visible append or branch switch
	runSuccess(t, dsn, f)
	post := inspect(t, dsn)
	compare(t, post, ref)
	evidence(t, tc.name, tc.boundary, pre, post, ref)
}

func TestRealProcessCrashBoundaries(t *testing.T) {
	cases := []crashCase{
		{name: "before_range", boundary: "before fetching a range", point: "before_range_fetch", final: "A", head: 4},
		{name: "during_range_rpc", boundary: "during RPC range fetch", hold: "range_logs", final: "A", head: 4},
		{name: "after_range_fetch", boundary: "after RPC data before validation", point: "after_range_fetch", final: "A", head: 4},
		{name: "after_validation", boundary: "after validation before DB transaction", point: "after_range_validated", final: "A", head: 4},
		{name: "append_first_block", boundary: "transaction after first block and log", point: "append_after_first_block", final: "A", head: 4},
		{name: "append_before_checkpoint", boundary: "before checkpoint update", point: "append_before_checkpoint", final: "A", head: 4},
		{name: "append_before_commit", boundary: "after checkpoint update before commit", point: "append_before_commit", final: "A", head: 4},
		{name: "append_after_commit", boundary: "after commit before success observed", point: "append_after_commit", final: "A", head: 4},
		{name: "between_ranges", boundary: "between two durable ranges", point: "between_ranges", final: "A", head: 65},
		{name: "ancestor_search", boundary: "during common ancestor search", point: "during_ancestor_search", pre: []string{"A"}, final: "B", head: 4},
		{name: "branch_fetch", boundary: "during reorg branch RPC fetch", hold: "branch_logs", pre: []string{"A"}, final: "B", head: 4},
		{name: "reorg_orphan", boundary: "after orphan update before activation", point: "reorg_after_orphan", pre: []string{"A"}, final: "B", head: 4},
		{name: "reorg_activate", boundary: "after first replacement activation", point: "reorg_after_first_activation", pre: []string{"A"}, final: "B", head: 4},
		{name: "reorg_before_commit", boundary: "after replacement checkpoint before commit", point: "reorg_before_commit", pre: []string{"A"}, final: "B", head: 4},
		{name: "reorg_after_commit", boundary: "after reorg commit before success observed", point: "reorg_after_commit", pre: []string{"A"}, final: "B", head: 4},
		{name: "aba_return", boundary: "during B to A re-canonicalization", point: "reorg_after_first_activation", pre: []string{"A", "B"}, final: "A", head: 4},
		{name: "empty_logs", boundary: "empty-log range transaction", point: "append_after_first_block", final: "A", head: 4, empty: true},
		{name: "duplicate_logs", boundary: "re-delivered logs in transaction", point: "append_after_first_block", final: "A", head: 4, duplicate: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { exerciseCrash(t, tc, nil) })
	}
}

func TestReorgProcessKillRepeat10(t *testing.T) {
	f := newRPCFixture(t, false, false)
	ref := referenceRun(t, f, []string{"A", "B"}, 4)
	for i := 1; i <= 10; i++ {
		t.Run("run_"+quantity(uint64(i)), func(t *testing.T) {
			exerciseCrash(t, crashCase{name: "reorg_kill_repeat", boundary: "reorg after orphan before replacement", point: "reorg_after_orphan", pre: []string{"A"}, final: "B", head: 4}, &ref)
		})
	}
}

func TestSIGTERMGracefulCancellation(t *testing.T) {
	f := newRPCFixture(t, false, false)
	ref := referenceRun(t, f, []string{"A"}, 4)
	dsn := schemaDSN(t)
	f.setBranch("A", 4)
	f.holdAt("range_logs")
	p := startChild(t, dsn, f, nil, "")
	waitRPC(t, f, "range_logs")
	pre := inspect(t, dsn)
	p.term(t)
	f.releaseHold()
	crashed := inspect(t, dsn)
	compare(t, crashed, pre)
	runSuccess(t, dsn, f)
	post := inspect(t, dsn)
	compare(t, post, ref)
	evidence(t, "sigterm_rpc_cancel", "SIGTERM during RPC fetch", pre, post, ref)
}

func TestSIGTERMDuringUncommittedTransaction(t *testing.T) {
	f := newRPCFixture(t, false, false)
	ref := referenceRun(t, f, []string{"A"}, 4)
	dsn := schemaDSN(t)
	f.setBranch("A", 4)
	control := newController(t)
	p := startChild(t, dsn, f, control, "append_before_commit")
	ev := waitProbe(t, control, "append_before_commit")
	defer ev.conn.Close()
	pre := inspect(t, dsn)
	p.term(t)
	crashed := inspect(t, dsn)
	compare(t, crashed, pre)
	runSuccess(t, dsn, f)
	post := inspect(t, dsn)
	compare(t, post, ref)
	evidence(t, "sigterm_uncommitted", "SIGTERM after checkpoint update before commit", pre, post, ref)
}

func dockerAction(t *testing.T, action, cid string) {
	t.Helper()
	cmd := exec.Command("docker", action, cid)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("docker %s failed: %v %s", action, err, out)
	}
}
func TestPostgreSQLTemporaryOutage(t *testing.T) {
	cid := os.Getenv("REORGGUARD_TEST_PG_CONTAINER")
	if cid == "" {
		t.Fatal("REORGGUARD_TEST_PG_CONTAINER required")
	}
	f := newRPCFixture(t, false, false)
	ref := referenceRun(t, f, []string{"A"}, 4)
	dsn := schemaDSN(t)
	f.setBranch("A", 4)
	control := newController(t)
	probe, err := store.Open(context.Background(), dsn, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Close()
	p := startChild(t, dsn, f, control, "after_range_validated")
	ev := waitProbe(t, control, "after_range_validated")
	defer ev.conn.Close()
	pre := inspect(t, dsn)
	dockerAction(t, "pause", cid)
	paused := true
	defer func() {
		if paused {
			dockerAction(t, "unpause", cid)
		}
	}()
	checkCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	state, readErr := (&reorg.Engine{Store: probe, ChainID: chainID}).Readiness(checkCtx)
	cancel()
	if readErr == nil || state.State != "unsafe" {
		t.Fatalf("DB outage readiness %+v %v", state, readErr)
	}
	if _, err = ev.conn.Write([]byte("go\n")); err != nil {
		t.Fatal(err)
	}
	p.wait(t, false) // the 10-second DB I/O deadline is the bound, not an infinite retry
	dockerAction(t, "unpause", cid)
	paused = false
	crashed := inspect(t, dsn)
	compare(t, crashed, pre)
	runSuccess(t, dsn, f)
	post := inspect(t, dsn)
	compare(t, post, ref)
	evidence(t, "postgres_outage", "PostgreSQL unavailable during append", pre, post, ref)
}
