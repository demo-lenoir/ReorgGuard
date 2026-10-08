// Package operational serves the bounded read-only HTTP API.
package operational

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"go.opentelemetry.io/otel/attribute"
	"reorgguard/internal/backfill"
	"reorgguard/internal/live"
	"reorgguard/internal/model"
	"reorgguard/internal/multirpc"
	"reorgguard/internal/reorg"
	"reorgguard/internal/rpc"
	"reorgguard/internal/store"
	"reorgguard/internal/telemetry"
)

type Repository interface {
	Snapshot(context.Context, uint64) (store.Snapshot, error)
	Readiness(context.Context, uint64) (store.Readiness, error)
	ListLogs(context.Context, uint64, store.LogQuery) (store.LogPage, error)
}
type LiveStatus interface {
	Status() live.Status
	WritePrometheus(io.Writer) error
}
type Server struct {
	ChainID      uint64
	Store        Repository
	Live         LiveStatus
	Pool         *multirpc.Pool
	Reorg        *reorg.Metrics
	Backfill     *backfill.Metrics
	RPC          *rpc.HTTPClient
	WSConfigured bool
	Logger       *slog.Logger
}
type cursor struct {
	Revision uint64       `json:"r"`
	Key      store.LogKey `json:"k"`
	Filter   string       `json:"f"`
}

// NewHTTPServer applies the actual socket write bound in addition to the
// handler's database/request context bound. Cancellation alone cannot stop a
// write to a client that has stopped reading.
func NewHTTPServer(handler http.Handler) *http.Server {
	return &http.Server{Handler: handler, ReadHeaderTimeout: 3 * time.Second,
		WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	capacity := make(chan struct{}, 32)
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "live"}) })
	mux.HandleFunc("GET /health/ready", s.ready)
	mux.HandleFunc("GET /v1/status", s.status)
	mux.HandleFunc("GET /v1/logs", s.logs)
	mux.HandleFunc("GET /metrics", s.metrics)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health/live" {
			select {
			case capacity <- struct{}{}:
				defer func() { <-capacity }()
			default:
				writeError(w, 429, "RATE_LIMITED", "Too many concurrent requests")
				return
			}
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		route := safeRoute(r.URL.Path)
		ctx, span := telemetry.Start(ctx, "api.request", attribute.String("http.route", route))
		defer span.End()
		start := time.Now()
		ww := &statusWriter{ResponseWriter: w, code: 200}
		mux.ServeHTTP(ww, r.WithContext(ctx))
		logger := s.Logger
		if logger == nil {
			logger = slog.Default()
		}
		logger.Info("operational request", "operation", route, "method", r.Method, "status", ww.code, "duration_ms", time.Since(start).Milliseconds())
	})
}
func safeRoute(path string) string {
	switch path {
	case "/health/live", "/health/ready", "/v1/status", "/v1/logs", "/metrics":
		return path
	}
	return "unmatched"
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(code int) { w.code = code; w.ResponseWriter.WriteHeader(code) }

func (s *Server) snapshot(ctx context.Context) (store.Snapshot, live.Status, string, []map[string]any, error) {
	snap, err := s.Store.Snapshot(ctx, s.ChainID)
	if err != nil {
		return store.Snapshot{}, live.Status{}, "", nil, err
	}
	x := s.Live.Status()
	active := "primary"
	providers := []map[string]any{{"name": "primary", "state": providerState(x), "head": x.RemoteHead}}
	if s.Pool != nil {
		p := s.Pool.Status()
		active = p.Active
		providers = make([]map[string]any, 0, len(p.Endpoints))
		for _, e := range p.Endpoints {
			providers = append(providers, map[string]any{"name": e.ID, "state": e.State, "head": e.Head})
		}
	}
	return snap, x, active, providers, nil
}
func providerState(x live.Status) string {
	if x.Ready {
		return "healthy"
	}
	return "temporarily_unavailable"
}
func (s *Server) readiness(ctx context.Context) (map[string]any, int) {
	r, err := s.Store.Readiness(ctx, s.ChainID)
	if err != nil {
		return map[string]any{"ready": false, "state": "db_unavailable", "reason": "db_unavailable"}, 503
	}
	if r.State != "healthy" {
		return map[string]any{"ready": false, "state": "unsafe", "reason": r.Reason}, 503
	}
	x := s.Live.Status()
	if !x.Ready {
		reason := x.Reason
		if reason == "" {
			reason = "progress_unavailable"
		}
		return map[string]any{"ready": false, "state": x.State, "reason": reason}, 503
	}
	return map[string]any{"ready": true, "state": x.State, "reason": ""}, 200
}
func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	data, code := s.readiness(r.Context())
	writeJSON(w, code, data)
}
func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	snap, x, active, providers, err := s.snapshot(r.Context())
	if err != nil {
		writeError(w, 503, "DATABASE_UNAVAILABLE", "Durable status unavailable")
		return
	}
	readiness, code := s.readiness(r.Context())
	ws := "disabled"
	if s.WSConfigured {
		ws = "degraded"
		if x.WSConnected {
			ws = "connected"
		}
	}
	age := time.Since(snap.UpdatedAt).Seconds()
	if age < 0 {
		age = 0
	}
	lag := uint64(0)
	if x.RemoteHead > snap.Checkpoint.Number {
		lag = x.RemoteHead - snap.Checkpoint.Number
	}
	out := map[string]any{"chain_id": s.ChainID, "remote_head": x.RemoteHead, "indexed_head": snap.Checkpoint.Number, "lag_blocks": lag, "canonical_head_hash": snap.Checkpoint.Hash.Hex(), "checkpoint_age_seconds": age, "active_rpc_endpoint": active, "http_providers": providers, "websocket_state": ws, "sync_state": readiness["state"], "ready": code == 200, "reason": readiness["reason"], "last_reorg": json.RawMessage(snap.LastReorg)}
	if len(snap.LastReorg) == 0 {
		out["last_reorg"] = nil
	}
	writeJSON(w, 200, out)
}
func (s *Server) logs(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	for key, values := range query {
		switch key {
		case "from_block", "to_block", "address", "topic0", "limit", "cursor":
		default:
			writeError(w, 400, "INVALID_ARGUMENT", "Unknown query parameter")
			return
		}
		if len(values) != 1 || values[0] == "" {
			writeError(w, 400, "INVALID_ARGUMENT", "Invalid query parameter")
			return
		}
	}
	from, err := parseNumber(query.Get("from_block"), 0)
	if err != nil {
		writeError(w, 400, "INVALID_ARGUMENT", "Invalid from_block")
		return
	}
	to, err := parseNumber(query.Get("to_block"), math.MaxInt64)
	if err != nil || from > to {
		writeError(w, 400, "INVALID_ARGUMENT", "Invalid block range")
		return
	}
	limit, err := parseNumber(query.Get("limit"), 50)
	if err != nil || limit < 1 || limit > 100 {
		writeError(w, 400, "INVALID_ARGUMENT", "Invalid limit")
		return
	}
	q := store.LogQuery{From: from, To: to, Limit: int(limit)}
	if raw := query.Get("address"); raw != "" {
		if !common.IsHexAddress(raw) || len(raw) != 42 {
			writeError(w, 400, "INVALID_ARGUMENT", "Invalid address")
			return
		}
		a := common.HexToAddress(raw)
		q.Address = &a
	}
	if raw := query.Get("topic0"); raw != "" {
		if !validHash(raw) {
			writeError(w, 400, "INVALID_ARGUMENT", "Invalid topic0")
			return
		}
		t := common.HexToHash(raw)
		q.Topic0 = &t
	}
	fingerprint := filterFingerprint(q)
	if raw := query.Get("cursor"); raw != "" {
		if len(raw) > 512 {
			writeError(w, 400, "INVALID_ARGUMENT", "Invalid cursor")
			return
		}
		var c cursor
		data, e := base64.RawURLEncoding.DecodeString(raw)
		if e != nil || json.Unmarshal(data, &c) != nil || c.Filter != fingerprint || c.Key.BlockHash == (common.Hash{}) {
			writeError(w, 400, "INVALID_ARGUMENT", "Invalid cursor")
			return
		}
		q.After = &c.Key
		q.Revision = &c.Revision
	}
	page, err := s.Store.ListLogs(r.Context(), s.ChainID, q)
	if errors.Is(err, store.ErrRevisionChanged) {
		writeError(w, 409, "CANONICAL_REVISION_CHANGED", "Canonical branch changed; restart pagination")
		return
	}
	if errors.Is(err, store.ErrPageTooLarge) {
		writeError(w, 413, "RESULT_TOO_LARGE", "Log payload exceeds API response limit")
		return
	}
	if err != nil {
		if errors.Is(err, model.ErrInvalidRange) {
			writeError(w, 400, "INVALID_ARGUMENT", "Invalid query")
			return
		}
		writeError(w, 503, "DATABASE_UNAVAILABLE", "Log page unavailable")
		return
	}
	items := make([]map[string]any, 0, len(page.Items))
	for _, l := range page.Items {
		topics := make([]string, 0, len(l.Topics))
		for _, t := range l.Topics {
			topics = append(topics, t.Hex())
		}
		items = append(items, map[string]any{"block_number": l.BlockNumber, "block_hash": l.BlockHash.Hex(), "transaction_hash": l.TxHash.Hex(), "transaction_index": l.TxIndex, "log_index": l.Index, "address": l.Address.Hex(), "topics": topics, "data": "0x" + hex.EncodeToString(l.Data)})
	}
	var next any
	if page.Next != nil {
		data, _ := json.Marshal(cursor{Revision: page.Revision, Key: *page.Next, Filter: fingerprint})
		next = base64.RawURLEncoding.EncodeToString(data)
	}
	writeJSON(w, 200, map[string]any{"items": items, "next_cursor": next, "canonical_revision": page.Revision})
}
func parseNumber(raw string, fallback uint64) (uint64, error) {
	if raw == "" {
		return fallback, nil
	}
	n, e := strconv.ParseUint(raw, 10, 63)
	return n, e
}
func validHash(raw string) bool {
	if len(raw) != 66 || !strings.HasPrefix(raw, "0x") {
		return false
	}
	_, e := hex.DecodeString(raw[2:])
	return e == nil
}
func filterFingerprint(q store.LogQuery) string {
	a, t := "", ""
	if q.Address != nil {
		a = q.Address.Hex()
	}
	if q.Topic0 != nil {
		t = q.Topic0.Hex()
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d/%d/%s/%s/%d", q.From, q.To, a, t, q.Limit)))
	return hex.EncodeToString(sum[:8])
}
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
func writeError(w http.ResponseWriter, code int, kind, msg string) {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	writeJSON(w, code, map[string]string{"code": kind, "message": msg, "request_id": hex.EncodeToString(b)})
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	snap, x, _, _, err := s.snapshot(r.Context())
	if err == nil {
		lag := uint64(0)
		if x.RemoteHead > snap.Checkpoint.Number {
			lag = x.RemoteHead - snap.Checkpoint.Number
		}
		age := time.Since(snap.UpdatedAt).Seconds()
		if age < 0 {
			age = 0
		}
		_, _ = fmt.Fprintf(w, "reorgguard_remote_head_block %d\nreorgguard_indexed_head_block %d\nreorgguard_lag_blocks %d\nreorgguard_checkpoint_age_seconds %g\n", x.RemoteHead, snap.Checkpoint.Number, lag, age)
	} else {
		x = s.Live.Status()
		_, _ = fmt.Fprintf(w, "reorgguard_remote_head_block %d\nreorgguard_indexed_head_block NaN\nreorgguard_lag_blocks NaN\nreorgguard_checkpoint_age_seconds NaN\n", x.RemoteHead)
	}
	_ = s.Live.WritePrometheus(w)
	if s.Pool != nil {
		_ = s.Pool.WritePrometheus(w)
	} else if s.RPC != nil {
		_ = s.RPC.WritePrometheus(w, "primary")
		state := "temporarily_unavailable"
		if x.Ready {
			state = "healthy"
		}
		_, _ = fmt.Fprint(w, "reorgguard_rpc_failovers_total 0\nreorgguard_rpc_incompatibilities_total 0\nreorgguard_rpc_stale_detections_total 0\nreorgguard_rpc_endpoint_selected{endpoint=\"primary\"} 1\n")
		for _, candidate := range []string{"healthy", "degraded", "rate_limited", "stale", "incompatible", "temporarily_unavailable", "recovering"} {
			value := 0
			if candidate == state {
				value = 1
			}
			_, _ = fmt.Fprintf(w, "reorgguard_rpc_endpoint_state{endpoint=%q,state=%q} %d\n", "primary", candidate, value)
		}
		_, _ = fmt.Fprint(w, "reorgguard_active_backfill_workers{endpoint=\"primary\"} 0\nreorgguard_inflight_backfill_ranges{endpoint=\"primary\"} 0\nreorgguard_pending_ordered_results{endpoint=\"primary\"} 0\nreorgguard_backfill_backpressure_total{endpoint=\"primary\"} 0\n")
	}
	if s.Reorg != nil {
		_ = s.Reorg.WritePrometheus(w)
	}
	if s.Backfill != nil {
		_ = s.Backfill.WritePrometheus(w)
	}
}
