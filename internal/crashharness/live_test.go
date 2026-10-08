//go:build crash && live

package crashharness

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5"
)

type liveWS struct {
	mu        sync.Mutex
	conn      *websocket.Conn
	connected chan struct{}
	server    *httptest.Server
	enabled   atomic.Bool
	lastChild *child
}

func newLiveWS(t *testing.T) *liveWS {
	t.Helper()
	w := &liveWS{connected: make(chan struct{}, 20)}
	w.enabled.Store(true)
	w.server = httptest.NewServer(http.HandlerFunc(func(out http.ResponseWriter, req *http.Request) {
		if !w.enabled.Load() {
			http.Error(out, "unavailable", http.StatusServiceUnavailable)
			return
		}
		conn, err := (&websocket.Upgrader{}).Upgrade(out, req, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		w.mu.Lock()
		w.conn = conn
		w.mu.Unlock()
		defer func() {
			w.mu.Lock()
			if w.conn == conn {
				w.conn = nil
			}
			w.mu.Unlock()
		}()
		for i := 1; i <= 4; i++ {
			var call struct {
				ID     int    `json:"id"`
				Method string `json:"method"`
			}
			if conn.ReadJSON(&call) != nil {
				return
			}
			var result any
			switch i {
			case 1:
				result = "0x7a69"
			case 2:
				result = rpcBlockObject(fixtureBlock(0, 1, common.Hash{}))
			case 3:
				result = "heads"
			case 4:
				result = "logs"
			}
			if conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": call.ID, "result": result}) != nil {
				return
			}
		}
		select {
		case w.connected <- struct{}{}:
		default:
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(w.server.Close)
	return w
}
func (w *liveWS) URL() string { return "ws" + strings.TrimPrefix(w.server.URL, "http") }
func (w *liveWS) await(t *testing.T) {
	t.Helper()
	select {
	case <-w.connected:
	case <-time.After(5 * time.Second):
		if w.lastChild != nil {
			select {
			case err := <-w.lastChild.done:
				t.Fatalf("WS did not subscribe; child exited: %v\n%s", err, w.lastChild.output.String())
			default:
			}
		}
		t.Fatal("WS did not subscribe; child still running")
	}
}
func (w *liveWS) head(t *testing.T, n uint64, hash common.Hash, parent common.Hash) {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.conn == nil {
		t.Fatal("WS not connected")
	}
	err := w.conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "method": "eth_subscription", "params": map[string]any{"subscription": "heads", "result": map[string]any{"number": quantity(n), "hash": hash.Hex(), "parentHash": parent.Hex(), "timestamp": quantity(n + 1)}}})
	if err != nil {
		t.Fatal(err)
	}
}
func (w *liveWS) disconnect() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.conn != nil {
		_ = w.conn.Close()
		w.conn = nil
	}
}
func (w *liveWS) logHint(t *testing.T, l any) {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.conn == nil {
		t.Fatal("WS not connected")
	}
	if err := w.conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "method": "eth_subscription", "params": map[string]any{"subscription": "logs", "result": l}}); err != nil {
		t.Fatal(err)
	}
}
func startLiveChild(t *testing.T, dsn string, f *rpcFixture, w *liveWS, c *probeController, point, poll string, extraEnv ...string) *child {
	t.Helper()
	binary := os.Getenv("REORGGUARD_CRASH_BINARY")
	if binary == "" {
		t.Fatal("crash binary required")
	}
	p := newChild(exec.Command(binary))
	p.cmd.Env = append(os.Environ(), "REORGGUARD_RPC_HTTP_URL="+f.server.URL, "REORGGUARD_RPC_WS_URL="+w.URL(), "REORGGUARD_SYNC_MODE=live", "REORGGUARD_POLL_INTERVAL="+poll, "REORGGUARD_DATABASE_URL="+dsn, "REORGGUARD_CHAIN_ID=31337", "REORGGUARD_GENESIS_HASH="+h(1).Hex(), "REORGGUARD_START_BLOCK=1", "REORGGUARD_ADDRESS="+(common.Address{19: 1}).Hex(), "REORGGUARD_MAX_REORG_DEPTH=4")
	p.cmd.Env = append(p.cmd.Env, extraEnv...)
	if point != "" {
		p.cmd.Env = append(p.cmd.Env, "REORGGUARD_CRASHPROBE_POINT="+point, "REORGGUARD_CRASHPROBE_ADDR="+c.ln.Addr().String())
	}
	p.cmd.Stdout = &p.output
	p.cmd.Stderr = &p.output
	w.lastChild = p
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go p.reap()
	t.Cleanup(func() {
		select {
		case <-p.reaped:
			return
		default:
			_ = p.cmd.Process.Kill()
			select {
			case <-p.reaped:
			case <-time.After(3 * time.Second):
				t.Error("live child not reaped")
			}
		}
	})
	return p
}
func waitHead(t *testing.T, dsn string, number int64, hash common.Hash) snapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		var gotNumber int64
		var gotHash []byte
		err := conn.QueryRow(ctx, "SELECT checkpoint_number,checkpoint_hash FROM sync_state WHERE chain_id=31337").Scan(&gotNumber, &gotHash)
		if err == nil && gotNumber == number && common.BytesToHash(gotHash) == hash {
			return inspect(t, dsn)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("head %d/%s not reached: %v", number, hash, ctx.Err())
			return snapshot{}
		case <-tick.C:
		}
	}
}
func TestLiveBinarySIGKILLDuringCatchup(t *testing.T) {
	f := newRPCFixture(t, false, false)
	f.setBranch("A", 0)
	w := newLiveWS(t)
	dsn := schemaDSN(t)
	p := startLiveChild(t, dsn, f, w, nil, "", "1h")
	w.await(t)
	waitHead(t, dsn, 0, h(1))
	p.term(t)
	f.setBranch("A", 5)
	reference := referenceRun(t, f, []string{"A"}, 5)
	f.setBranch("A", 0)
	// Resume at zero, then require the WS hint to start the five-block range.
	c := newController(t)
	p = startLiveChild(t, dsn, f, w, c, "append_before_checkpoint", "1h")
	w.await(t)
	f.setBranch("A", 5)
	w.head(t, 5, h(6), h(5))
	ev := waitProbe(t, c, "append_before_checkpoint")
	pre := inspect(t, dsn)
	if pre.cpNumber != 0 {
		t.Fatalf("pre-crash checkpoint %d", pre.cpNumber)
	}
	p.kill(t)
	_ = ev.conn.Close()
	p = startLiveChild(t, dsn, f, w, nil, "", "25ms")
	w.await(t)
	post := waitHead(t, dsn, 5, h(6))
	compare(t, post, reference)
	evidence(t, "live_sigkill_catchup", "append_before_checkpoint", pre, post, reference)
	p.term(t)
}
func TestLiveBinaryReorgReconnectABA(t *testing.T) {
	f := newRPCFixture(t, false, false)
	f.setBranch("A", 4)
	w := newLiveWS(t)
	dsn := schemaDSN(t)
	reference := referenceRun(t, f, []string{"A", "B", "A"}, 4)
	f.setBranch("A", 4)
	p := startLiveChild(t, dsn, f, w, nil, "", "25ms")
	w.await(t)
	a := waitHead(t, dsn, 4, h(5))
	f.setBranch("B", 4)
	w.head(t, 4, h(104), h(103))
	b := waitHead(t, dsn, 4, h(104))
	if b.orphanBlocks == 0 {
		t.Fatal("old A branch not retained")
	}
	// A removed log from orphaned A is a hint; B stays canonical.
	removed := rpcLogObject(f.logs[h(4)][0]).(map[string]any)
	removed["removed"] = true
	w.logHint(t, removed)
	stillB := waitHead(t, dsn, 4, h(104))
	if stillB.checksum != b.checksum {
		t.Fatal("removed log directly changed canonical rows")
	}
	w.enabled.Store(false)
	w.disconnect()
	f.setBranch("A", 4)
	final := waitHead(t, dsn, 4, h(5))
	w.enabled.Store(true)
	w.await(t)
	compare(t, final, reference)
	evidence(t, "live_reorg_aba_disconnect", "http_poll_fallback", a, final, reference)
	p.term(t)
}

func TestLiveBinaryReconnectGap(t *testing.T) {
	for _, gap := range []uint64{0, 3} {
		t.Run(quantity(gap), func(t *testing.T) {
			f := newRPCFixture(t, false, false)
			f.setBranch("A", 2)
			w := newLiveWS(t)
			dsn := schemaDSN(t)
			p := startLiveChild(t, dsn, f, w, nil, "", "1h")
			w.await(t)
			pre := waitHead(t, dsn, 2, h(3))
			w.enabled.Store(false)
			w.disconnect()
			f.setBranch("A", 2+gap)
			if during := inspect(t, dsn); during.cpNumber != 2 || during.cpHash != h(3) {
				t.Fatalf("checkpoint moved while WS disabled: %+v", during)
			}
			w.enabled.Store(true)
			w.await(t)
			post := waitHead(t, dsn, int64(2+gap), h(byte(3+gap)))
			p.term(t)
			reference := referenceRun(t, f, []string{"A"}, 2+gap)
			compare(t, post, reference)
			evidence(t, "live_reconnect_gap_"+quantity(gap), "ws_reconnect_http_sweep", pre, post, reference)
		})
	}
}
