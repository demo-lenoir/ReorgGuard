package operational

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"reorgguard/internal/backfill"
	"reorgguard/internal/live"
	"reorgguard/internal/model"
	"reorgguard/internal/reorg"
	"reorgguard/internal/rpc"
	"reorgguard/internal/store"
)

type fakeStore struct {
	rows     []model.Log
	revision uint64
	broken   bool
	unsafe   bool
	pageErr  error
	entered  chan struct{}
	release  chan struct{}
}

func (f *fakeStore) Snapshot(ctx context.Context, _ uint64) (store.Snapshot, error) {
	if f.broken {
		return store.Snapshot{}, errors.New("secret database URL")
	}
	return store.Snapshot{Checkpoint: store.Checkpoint{Number: 3, Hash: common.HexToHash("0x03")}, Revision: f.revision, UpdatedAt: time.Now()}, nil
}
func (f *fakeStore) Readiness(ctx context.Context, _ uint64) (store.Readiness, error) {
	if f.broken {
		return store.Readiness{}, errors.New("secret database URL")
	}
	if f.unsafe {
		return store.Readiness{State: "unsafe", Reason: "depth_exceeded"}, nil
	}
	return store.Readiness{State: "healthy"}, nil
}
func (f *fakeStore) ListLogs(ctx context.Context, _ uint64, q store.LogQuery) (store.LogPage, error) {
	if f.entered != nil {
		f.entered <- struct{}{}
		select {
		case <-f.release:
		case <-ctx.Done():
			return store.LogPage{}, ctx.Err()
		}
	}
	if f.pageErr != nil {
		return store.LogPage{}, f.pageErr
	}
	if ctx.Err() != nil {
		return store.LogPage{}, ctx.Err()
	}
	if f.broken {
		return store.LogPage{}, errors.New("secret database URL")
	}
	if q.Revision != nil && *q.Revision != f.revision {
		return store.LogPage{}, store.ErrRevisionChanged
	}
	out := store.LogPage{Items: []model.Log{}, Revision: f.revision}
	for _, l := range f.rows {
		if l.BlockNumber < q.From || l.BlockNumber > q.To || (q.Address != nil && l.Address != *q.Address) || (q.Topic0 != nil && (len(l.Topics) == 0 || l.Topics[0] != *q.Topic0)) {
			continue
		}
		if q.After != nil && (l.BlockNumber < q.After.BlockNumber || (l.BlockNumber == q.After.BlockNumber && l.Index <= q.After.LogIndex)) {
			continue
		}
		out.Items = append(out.Items, l)
	}
	if len(out.Items) > q.Limit {
		out.Items = out.Items[:q.Limit]
		last := out.Items[len(out.Items)-1]
		out.Next = &store.LogKey{BlockNumber: last.BlockNumber, LogIndex: last.Index, BlockHash: last.BlockHash}
	}
	return out, nil
}

type fakeLive struct{ x live.Status }

func (f fakeLive) Status() live.Status { return f.x }
func (f fakeLive) WritePrometheus(w io.Writer) error {
	_, err := io.WriteString(w, "reorgguard_ws_connected 0\n")
	return err
}
func request(t *testing.T, h http.Handler, path string) (int, map[string]any) {
	t.Helper()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	return w.Code, body
}
func TestLogsPaginationFiltersAndErrors(t *testing.T) {
	addr := common.HexToAddress("0x0000000000000000000000000000000000000001")
	topic := common.HexToHash("0x01")
	f := &fakeStore{revision: 4}
	for i := uint64(1); i <= 3; i++ {
		f.rows = append(f.rows, model.Log{BlockNumber: i, BlockHash: common.BigToHash(newBig(i)), TxHash: common.BigToHash(newBig(i + 10)), Index: 0, Address: addr, Topics: []common.Hash{topic}})
	}
	s := &Server{ChainID: 1, Store: f, Live: fakeLive{x: live.Status{Ready: true, State: "healthy_caught_up", RemoteHead: 3}}}
	h := s.Handler()
	code, body := request(t, h, "/v1/logs?limit=2&address="+addr.Hex()+"&topic0="+topic.Hex())
	if code != 200 || len(body["items"].([]any)) != 2 || body["next_cursor"] == nil {
		t.Fatalf("first page %d %+v", code, body)
	}
	c := body["next_cursor"].(string)
	code, body = request(t, h, "/v1/logs?limit=2&address="+addr.Hex()+"&topic0="+topic.Hex()+"&cursor="+c)
	if code != 200 || len(body["items"].([]any)) != 1 || body["next_cursor"] != nil {
		t.Fatalf("second page %d %+v", code, body)
	}
	for _, path := range []string{"/v1/logs?limit=0", "/v1/logs?limit=101", "/v1/logs?limit=", "/v1/logs?limit=1&limit=2", "/v1/logs?from_block=4&to_block=3", "/v1/logs?address=oops", "/v1/logs?topic0=oops", "/v1/logs?cursor=bad", "/v1/logs?other=1"} {
		code, _ = request(t, h, path)
		if code != 400 {
			t.Fatalf("%s: %d", path, code)
		}
	}
	code, body = request(t, h, "/v1/logs?from_block=99")
	if code != 200 || len(body["items"].([]any)) != 0 {
		t.Fatalf("empty page %+v", body)
	}
	f.revision++
	code, body = request(t, h, "/v1/logs?limit=2&address="+addr.Hex()+"&topic0="+topic.Hex()+"&cursor="+c)
	if code != 409 || body["code"] != "CANONICAL_REVISION_CHANGED" {
		t.Fatalf("revision %d %+v", code, body)
	}
}
func newBig(n uint64) *big.Int { return new(big.Int).SetUint64(n) }
func TestLivenessReadinessAndRedaction(t *testing.T) {
	f := &fakeStore{}
	x := fakeLive{x: live.Status{Ready: true, State: "ws_degraded_http_healthy", RemoteHead: 3}}
	s := &Server{ChainID: 1, Store: f, Live: x, WSConfigured: true}
	code, body := request(t, s.Handler(), "/health/ready")
	if code != 200 || body["ready"] != true {
		t.Fatalf("WS fallback %+v", body)
	}
	f.unsafe = true
	code, body = request(t, s.Handler(), "/health/ready")
	if code != 503 || body["reason"] != "depth_exceeded" {
		t.Fatalf("unsafe %+v", body)
	}
	f.unsafe = false
	f.broken = true
	code, body = request(t, s.Handler(), "/health/ready")
	if code != 503 || body["state"] != "db_unavailable" {
		t.Fatalf("db %+v", body)
	}
	code, body = request(t, s.Handler(), "/health/live")
	if code != 200 || body["status"] != "live" {
		t.Fatalf("live %+v", body)
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/v1/status", nil))
	if w.Code != 503 || strings.Contains(w.Body.String(), "secret database URL") {
		t.Fatalf("secret leak %d %s", w.Code, w.Body.String())
	}
}

func TestRequestCancellationDBTimeoutAndMetrics(t *testing.T) {
	f := &fakeStore{pageErr: context.DeadlineExceeded}
	client, err := rpc.NewHTTPClient("http://127.0.0.1:1", time.Second, 4096)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{ChainID: 1, Store: f, Live: fakeLive{x: live.Status{Ready: true, State: "healthy_caught_up", RemoteHead: 3}}, RPC: client, Reorg: reorg.NewMetrics(), Backfill: &backfill.Metrics{}}
	h := s.Handler()
	code, body := request(t, h, "/v1/logs")
	if code != 503 || body["code"] != "DATABASE_UNAVAILABLE" {
		t.Fatalf("DB timeout %d %+v", code, body)
	}
	f.pageErr = nil
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/v1/logs", nil).WithContext(ctx))
	if w.Code != 503 {
		t.Fatalf("canceled request %d", w.Code)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if w.Code != 200 {
		t.Fatalf("metrics %d", w.Code)
	}
	for _, name := range []string{"reorgguard_remote_head_block", "reorgguard_indexed_head_block", "reorgguard_lag_blocks", "reorgguard_checkpoint_age_seconds", "reorgguard_backfill_range_blocks", "reorgguard_logs_processed_total", "reorgguard_duplicate_logs_total", "reorgguard_reorgs_total", "reorgguard_reorg_depth_blocks", "reorgguard_rpc_requests_total{endpoint=\"primary\"", "reorgguard_rpc_latency_seconds{endpoint=\"primary\"", "reorgguard_ws_connected"} {
		if !strings.Contains(w.Body.String(), name) {
			t.Fatalf("missing metric %s", name)
		}
	}
	if strings.Contains(w.Body.String(), "127.0.0.1:1") {
		t.Fatal("RPC URL in metrics")
	}
	f.pageErr = store.ErrPageTooLarge
	code, body = request(t, h, "/v1/logs")
	if code != 413 || body["code"] != "RESULT_TOO_LARGE" {
		t.Fatalf("oversized log %d %+v", code, body)
	}
	f.pageErr = nil
	f.broken = true
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), "reorgguard_indexed_head_block NaN") || strings.Contains(w.Body.String(), "secret database URL") {
		t.Fatalf("DB outage metrics: %d %s", w.Code, w.Body.String())
	}
}

func TestReadinessMatrix(t *testing.T) {
	cases := []struct {
		name             string
		live             live.Status
		unsafe, dbBroken bool
		want             int
		state            string
	}{
		{"caught_up", live.Status{Ready: true, State: "healthy_caught_up"}, false, false, 200, "healthy_caught_up"},
		{"bounded_lag", live.Status{Ready: true, State: "healthy_lagging", Lag: 1}, false, false, 200, "healthy_lagging"},
		{"ws_degraded_http_fallback", live.Status{Ready: true, State: "ws_degraded_http_healthy", PollingFallbacks: 1}, false, false, 200, "ws_degraded_http_healthy"},
		{"http_unavailable", live.Status{Ready: false, State: "http_unavailable", Reason: "http_unavailable"}, false, false, 503, "http_unavailable"},
		{"lag_exceeded", live.Status{Ready: false, State: "lag_exceeded", Reason: "lag_exceeded", Lag: 3}, false, false, 503, "lag_exceeded"},
		{"unsafe_fork", live.Status{Ready: false, State: "unsafe", Reason: "depth_exceeded"}, true, false, 503, "unsafe"},
		{"db_unavailable", live.Status{Ready: true, State: "healthy_caught_up"}, false, true, 503, "db_unavailable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &Server{ChainID: 1, Store: &fakeStore{unsafe: tc.unsafe, broken: tc.dbBroken}, Live: fakeLive{x: tc.live}}
			code, body := request(t, s.Handler(), "/health/ready")
			if code != tc.want || body["state"] != tc.state {
				t.Fatalf("readiness %d %+v", code, body)
			}
			code, body = request(t, s.Handler(), "/health/live")
			if code != 200 || body["status"] != "live" {
				t.Fatalf("liveness %d %+v", code, body)
			}
		})
	}
}

func TestBoundedConcurrentAPIRequests(t *testing.T) {
	f := &fakeStore{entered: make(chan struct{}, 32), release: make(chan struct{})}
	s := &Server{ChainID: 1, Store: f, Live: fakeLive{x: live.Status{Ready: true, State: "healthy_caught_up"}}}
	h := s.Handler()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest("GET", "/v1/logs", nil))
			if w.Code != 200 {
				t.Errorf("blocked request returned %d", w.Code)
			}
		}()
	}
	for i := 0; i < 32; i++ {
		select {
		case <-f.entered:
		case <-time.After(3 * time.Second):
			t.Fatal("concurrent requests did not enter")
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/v1/logs", nil))
	if w.Code != 429 {
		t.Fatalf("expected bounded rejection, got %d", w.Code)
	}
	code, body := request(t, h, "/health/live")
	if code != 200 || body["status"] != "live" {
		t.Fatalf("liveness under load %d %+v", code, body)
	}
	close(f.release)
	wg.Wait()
}
