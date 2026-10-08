package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gorilla/websocket"
)

func wsHash(n byte) common.Hash { var h common.Hash; h[31] = n; return h }
func wsServer(t *testing.T, chain string, messages []any, interleave ...bool) (string, *atomic.Int32) {
	t.Helper()
	connections := &atomic.Int32{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		connections.Add(1)
		for i := 1; i <= 4; i++ {
			var request struct {
				ID     int    `json:"id"`
				Method string `json:"method"`
			}
			if err := conn.ReadJSON(&request); err != nil {
				return
			}
			if request.ID != i {
				t.Errorf("request id %d, want %d", request.ID, i)
				return
			}
			var result any
			switch i {
			case 1:
				result = chain
			case 2:
				result = map[string]any{"number": "0x0", "hash": wsHash(1).Hex(), "parentHash": common.Hash{}.Hex(), "timestamp": "0x1"}
			case 3:
				result = "heads"
			case 4:
				result = "logs"
			}
			if i == 4 && len(interleave) > 0 && interleave[0] {
				head := map[string]any{"number": "0x2", "hash": wsHash(3).Hex(), "parentHash": wsHash(2).Hex(), "timestamp": "0x3"}
				if err := conn.WriteJSON(wsNotification("heads", head)); err != nil {
					return
				}
			}
			if err := conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": i, "result": result}); err != nil {
				return
			}
		}
		for _, message := range messages {
			if err := conn.WriteJSON(message); err != nil {
				return
			}
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http"), connections
}
func wsNotification(id string, result any) any {
	return map[string]any{"jsonrpc": "2.0", "method": "eth_subscription", "params": map[string]any{"subscription": id, "result": result}}
}
func wsClient(t *testing.T, url string) *WSClient {
	t.Helper()
	c, err := NewWSClient(url, 31337, wsHash(1), Filter{Addresses: []common.Address{{19: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	c.Heartbeat = time.Second
	return c
}
func TestWSHintsAndRemovedAreOnlyHints(t *testing.T) {
	head := map[string]any{"number": "0x2", "hash": wsHash(3).Hex(), "parentHash": wsHash(2).Hex(), "timestamp": "0x3"}
	log := map[string]any{"blockHash": wsHash(3).Hex(), "blockNumber": "0x2", "transactionHash": wsHash(9).Hex(), "transactionIndex": "0x0", "logIndex": "0x0", "address": (common.Address{19: 1}).Hex(), "topics": []string{}, "data": "0x", "removed": true}
	url, _ := wsServer(t, "0x7a69", []any{wsNotification("heads", head), wsNotification("heads", head), wsNotification("logs", log)})
	c := wsClient(t, url)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var hints []Hint
	err := c.Subscribe(ctx, nil, func(h Hint) {
		hints = append(hints, h)
		if len(hints) == 3 {
			cancel()
		}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("subscribe: %v", err)
	}
	if len(hints) != 3 || hints[0] != hints[1] || hints[2].Kind != LogHint || !hints[2].Removed {
		t.Fatalf("hints %+v", hints)
	}
}
func TestWSWrongChainRejected(t *testing.T) {
	url, _ := wsServer(t, "0x1", nil)
	c := wsClient(t, url)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Subscribe(ctx, nil, func(Hint) {}); !errors.Is(err, ErrWSWrongChain) {
		t.Fatalf("wrong chain: %v", err)
	}
}
func TestWSMalformedNotificationRejected(t *testing.T) {
	url, _ := wsServer(t, "0x7a69", []any{wsNotification("heads", map[string]any{"number": "0x2", "hash": "bad"})})
	c := wsClient(t, url)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Subscribe(ctx, nil, func(Hint) {}); !errors.Is(err, ErrWSMalformed) {
		t.Fatalf("malformed: %v", err)
	}
}
func TestWSInterleavedHintDuringSubscribe(t *testing.T) {
	url, _ := wsServer(t, "0x7a69", nil, true)
	c := wsClient(t, url)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connected := false
	err := c.Subscribe(ctx, func() { connected = true; cancel() }, func(Hint) {})
	if !connected || !errors.Is(err, context.Canceled) {
		t.Fatalf("interleaved hint prevented subscription: %v", err)
	}
}
func FuzzParseWSNotification(f *testing.F) {
	seed, _ := json.Marshal(wsNotification("heads", map[string]any{"number": "0x2", "hash": wsHash(3).Hex(), "parentHash": wsHash(2).Hex(), "timestamp": "0x3"}))
	f.Add(seed)
	f.Add([]byte("not json"))
	f.Fuzz(func(t *testing.T, body []byte) { _, _ = parseWSNotification(body, "heads", "logs") })
}
