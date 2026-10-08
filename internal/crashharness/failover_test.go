//go:build crash && live && multirpc

package crashharness

import (
	"testing"
)

// Each child is the real binary, each restart uses the same PostgreSQL schema,
// and inspect independently checks the ordered canonical projection. The
// reference is a separate uninterrupted schema populated from the same RPC
// history. No checkpoint or table is reset after SIGKILL.
func TestMultiRPCBinaryCrash(t *testing.T) {
	for _, scenario := range []struct {
		name  string
		point string
		hold  bool
	}{
		{"provider_switch", "during_failover", false},
		{"inflight_fetch", "", true},
		{"ordered_result_wait", "parallel_out_of_order_buffered", true},
		{"database_transaction", "append_before_checkpoint", false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			primary := newRPCFixture(t, false, false)
			fallback := newRPCFixture(t, false, false)
			primary.setBranch("A", 2)
			fallback.setBranch("A", 2)
			w := newLiveWS(t)
			dsn := schemaDSN(t)
			extra := []string{"REORGGUARD_RPC_FALLBACK_URLS=" + fallback.server.URL, "REORGGUARD_BACKFILL_WORKERS=2", "REORGGUARD_MAX_INFLIGHT_RANGES=2"}
			initial := startLiveChild(t, dsn, primary, w, nil, "", "1h", extra...)
			w.await(t)
			waitHead(t, dsn, 2, h(3))
			initial.term(t)

			fallback.setBranch("A", 130)
			reference := referenceRun(t, fallback, []string{"A"}, 130)
			fallback.setBranch("A", 2)
			// Reopen a live child while the primary is still a compatible but
			// shorter source. Its initial sweep only has the durable head 2.
			c := newController(t)
			child := startLiveChild(t, dsn, primary, w, c, scenario.point, "1h", extra...)
			w.await(t)
			// Reaching this connection guarantees the initial sweep has ended.
			// The primary then becomes unreachable; the fallback is re-probed
			// against the durable checkpoint before any canonical work.
			primary.server.Close()
			fallback.setBranch("A", 130)
			if scenario.hold {
				fallback.holdAt("range_logs")
				fallback.mu.Lock()
				fallback.holdFrom = 3
				fallback.mu.Unlock()
			}
			w.head(t, 130, h(131), h(130))
			if scenario.point != "" {
				ev := waitProbe(t, c, scenario.point)
				pre := inspect(t, dsn)
				if pre.cpNumber != 2 || pre.cpHash != h(3) {
					t.Fatalf("checkpoint moved before %s: %+v", scenario.point, pre)
				}
				child.kill(t)
				_ = ev.conn.Close()
				fallback.releaseHold()
				child = startLiveChild(t, dsn, primary, w, nil, "", "25ms", extra...)
				w.await(t)
				post := waitHead(t, dsn, 130, h(131))
				compare(t, post, reference)
				evidence(t, "multirpc_"+scenario.name, scenario.point, pre, post, reference)
				child.term(t)
				return
			}
			waitRPC(t, fallback, "range_logs")
			pre := inspect(t, dsn)
			if pre.cpNumber != 2 || pre.cpHash != h(3) {
				t.Fatalf("checkpoint moved during fetch: %+v", pre)
			}
			child.kill(t)
			fallback.releaseHold()
			child = startLiveChild(t, dsn, primary, w, nil, "", "25ms", extra...)
			w.await(t)
			post := waitHead(t, dsn, 130, h(131))
			compare(t, post, reference)
			evidence(t, "multirpc_"+scenario.name, "inflight_eth_getLogs", pre, post, reference)
			child.term(t)
		})
	}
}
