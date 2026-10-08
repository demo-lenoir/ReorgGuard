// demo-proxy is a loopback-only fault switch used by the local Anvil demo.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

type switcher struct {
	mu           sync.Mutex
	ws, http     bool
	connections  map[*websocket.Conn]struct{}
	upstream     *url.URL
	proxy        *httputil.ReverseProxy
	archiveHash  string
	archiveBlock json.RawMessage
	archiveLogs  json.RawMessage
}

func main() {
	upstream, err := url.Parse(os.Getenv("DEMO_UPSTREAM"))
	if err != nil || upstream.Scheme != "http" || upstream.Host == "" {
		slog.Error("invalid local upstream")
		os.Exit(2)
	}
	addr := os.Getenv("DEMO_LISTEN")
	host, _, err := net.SplitHostPort(addr)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		slog.Error("invalid loopback listen address")
		os.Exit(2)
	}
	s := &switcher{ws: true, http: true, connections: make(map[*websocket.Conn]struct{}), upstream: upstream, proxy: httputil.NewSingleHostReverseProxy(upstream)}
	s.proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) { http.Error(w, "upstream unavailable", 503) }
	mux := http.NewServeMux()
	mux.HandleFunc("POST /control/ws/{state}", func(w http.ResponseWriter, r *http.Request) {
		state := r.PathValue("state")
		if state != "on" && state != "off" {
			http.Error(w, "bad state", 400)
			return
		}
		s.mu.Lock()
		s.ws = state == "on"
		if !s.ws {
			for c := range s.connections {
				_ = c.Close()
			}
		}
		s.mu.Unlock()
		w.WriteHeader(204)
	})
	mux.HandleFunc("POST /control/http/{state}", func(w http.ResponseWriter, r *http.Request) {
		state := r.PathValue("state")
		if state != "on" && state != "off" {
			http.Error(w, "bad state", 400)
			return
		}
		s.mu.Lock()
		s.http = state == "on"
		s.mu.Unlock()
		w.WriteHeader(204)
	})
	mux.HandleFunc("GET /state", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.ws {
			_, _ = w.Write([]byte("ws=on "))
		} else {
			_, _ = w.Write([]byte("ws=off "))
		}
		if s.http {
			_, _ = w.Write([]byte("http=on"))
		} else {
			_, _ = w.Write([]byte("http=off"))
		}
	})
	mux.HandleFunc("POST /control/archive", func(w http.ResponseWriter, r *http.Request) {
		var item struct {
			Hash  string          `json:"hash"`
			Block json.RawMessage `json:"block"`
			Logs  json.RawMessage `json:"logs"`
		}
		if json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&item) != nil || len(item.Hash) != 66 || len(item.Block) == 0 || len(item.Logs) == 0 {
			http.Error(w, "invalid archive", 400)
			return
		}
		s.mu.Lock()
		s.archiveHash = strings.ToLower(item.Hash)
		s.archiveBlock = append([]byte(nil), item.Block...)
		s.archiveLogs = append([]byte(nil), item.Logs...)
		s.mu.Unlock()
		w.WriteHeader(204)
	})
	mux.HandleFunc("/", s.serve)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("proxy listen failed")
		os.Exit(2)
	}
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 3 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		done, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(done)
	}()
	if err = server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("proxy failed")
		os.Exit(1)
	}
}
func (s *switcher) serve(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	wsEnabled, httpEnabled := s.ws, s.http
	s.mu.Unlock()
	if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		if !wsEnabled {
			http.Error(w, "WS disabled", 503)
			return
		}
		s.websocket(w, r)
		return
	}
	if !httpEnabled {
		http.Error(w, "HTTP disabled", 503)
		return
	}
	if r.Method == http.MethodPost && s.archivedRPC(w, r) {
		return
	}
	s.proxy.ServeHTTP(w, r)
}
func (s *switcher) archivedRPC(w http.ResponseWriter, r *http.Request) bool {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return false
	}
	r.Body = io.NopCloser(strings.NewReader(string(body)))
	var call struct {
		ID     json.RawMessage   `json:"id"`
		Method string            `json:"method"`
		Params []json.RawMessage `json:"params"`
	}
	if json.Unmarshal(body, &call) != nil || len(call.Params) == 0 {
		return false
	}
	var hash string
	var result json.RawMessage
	switch call.Method {
	case "eth_getBlockByHash":
		_ = json.Unmarshal(call.Params[0], &hash)
	case "eth_getLogs":
		var f struct {
			BlockHash string `json:"blockHash"`
		}
		_ = json.Unmarshal(call.Params[0], &f)
		hash = f.BlockHash
	default:
		return false
	}
	s.mu.Lock()
	if strings.ToLower(hash) == s.archiveHash && hash != "" {
		if call.Method == "eth_getBlockByHash" {
			result = s.archiveBlock
		} else {
			result = s.archiveLogs
		}
	}
	s.mu.Unlock()
	if result == nil {
		return false
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":`))
	_, _ = w.Write(call.ID)
	_, _ = w.Write([]byte(`,"result":`))
	_, _ = w.Write(result)
	_, _ = w.Write([]byte(`}`))
	return true
}
func (s *switcher) websocket(w http.ResponseWriter, r *http.Request) {
	wsURL := *s.upstream
	wsURL.Scheme = "ws"
	wsURL.Path = r.URL.Path
	wsURL.RawQuery = r.URL.RawQuery
	up, _, err := websocket.DefaultDialer.Dial(wsURL.String(), nil)
	if err != nil {
		http.Error(w, "WS upstream unavailable", 503)
		return
	}
	client, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
	if err != nil {
		_ = up.Close()
		return
	}
	s.mu.Lock()
	if !s.ws {
		s.mu.Unlock()
		_ = client.Close()
		_ = up.Close()
		return
	}
	s.connections[client] = struct{}{}
	s.mu.Unlock()
	defer func() { s.mu.Lock(); delete(s.connections, client); s.mu.Unlock(); _ = client.Close(); _ = up.Close() }()
	done := make(chan struct{}, 2)
	copyMessages := func(dst, src *websocket.Conn) {
		for {
			typ, data, e := src.ReadMessage()
			if e != nil || dst.WriteMessage(typ, data) != nil {
				break
			}
		}
		done <- struct{}{}
	}
	go copyMessages(up, client)
	go copyMessages(client, up)
	<-done
	_ = client.Close()
	_ = up.Close()
	<-done
}
