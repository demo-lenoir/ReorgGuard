package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

func TestHTTPMethodsAndParsing(t *testing.T) {
	hash := fmt.Sprintf("0x%064x", 1)
	parent := fmt.Sprintf("0x%064x", 2)
	txHash := fmt.Sprintf("0x%064x", 3)
	address := fmt.Sprintf("0x%040x", 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		var result any
		switch req.Method {
		case "eth_chainId":
			result = "0x7a69"
		case "eth_blockNumber":
			result = "0x1"
		case "eth_getBlockByNumber", "eth_getBlockByHash":
			result = map[string]any{"number": "0x1", "hash": hash, "parentHash": parent, "timestamp": "0x2"}
		case "eth_getLogs":
			result = []any{map[string]any{"blockHash": hash, "blockNumber": "0x1", "transactionHash": txHash, "transactionIndex": "0x0", "logIndex": "0x0", "address": address, "topics": []string{}, "data": "0x01", "removed": false}}
		default:
			t.Errorf("unexpected method %s", req.Method)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": result})
	}))
	defer server.Close()
	c, err := NewHTTPClient(server.URL, time.Second, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	chain, err := c.ChainID(context.Background())
	if err != nil || chain != 31337 {
		t.Fatalf("chain %d %v", chain, err)
	}
	head, err := c.BlockNumber(context.Background())
	if err != nil || head != 1 {
		t.Fatalf("head %d %v", head, err)
	}
	b, err := c.BlockByNumber(context.Background(), 1)
	if err != nil || b.Number != 1 || b.Hash != common.HexToHash(hash) {
		t.Fatalf("block %+v %v", b, err)
	}
	_, err = c.BlockByHash(context.Background(), common.HexToHash(hash))
	if err != nil {
		t.Fatal(err)
	}
	logs, err := c.Logs(context.Background(), 1, 1, Filter{Addresses: []common.Address{common.HexToAddress(address)}})
	if err != nil || len(logs) != 1 || logs[0].Data[0] != 1 {
		t.Fatalf("logs %+v %v", logs, err)
	}
}
func TestLimitClassificationAndBounds(t *testing.T) {
	if err := (Filter{Topics: [][]common.Hash{{}}}).Validate(); err == nil {
		t.Fatal("unbounded wildcard filter accepted")
	}
	cases := []struct {
		err  error
		want bool
	}{
		{&RPCError{Code: -32005, Message: "provider limit"}, true},
		{&RPCError{Code: -32602, Data: json.RawMessage(`{"reason":"too_many_results"}`)}, true},
		{&RPCError{Code: -32000, Message: "query returned more than 10000 results"}, true},
		{&RPCError{Code: -32602, Message: "invalid address"}, false},
		{errors.New("too many results"), false},
	}
	for _, tc := range cases {
		if IsRangeLimit(tc.err) != tc.want {
			t.Fatalf("classify %v", tc.err)
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":"`)
		for range 2048 {
			fmt.Fprint(w, "x")
		}
		fmt.Fprint(w, `"}`)
	}))
	defer server.Close()
	c, err := NewHTTPClient(server.URL, time.Second, 1024)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.ChainID(context.Background())
	if !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("oversized: %v", err)
	}
}
func TestHTTPContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(100 * time.Millisecond):
		}
	}))
	defer server.Close()
	c, err := NewHTTPClient(server.URL, time.Second, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = c.BlockNumber(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancellation: %v", err)
	}
}
func TestHTTPRejectsNullLogsAndWrongBlockNumber(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Method == "eth_getLogs" {
			fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":null}`)
			return
		}
		fmt.Fprintf(w, `{"jsonrpc":"2.0","id":1,"result":{"number":"0x2","hash":"0x%064x","parentHash":"0x%064x","timestamp":"0x1"}}`, 1, 0)
	}))
	defer server.Close()
	c, err := NewHTTPClient(server.URL, time.Second, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Logs(context.Background(), 1, 1, Filter{Addresses: []common.Address{{19: 1}}})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("null logs accepted: %v", err)
	}
	_, err = c.BlockByNumber(context.Background(), 1)
	if !errors.Is(err, ErrMalformed) {
		t.Fatalf("wrong block number accepted: %v", err)
	}
}
func TestLogsByBlockHashBindsQueryAndResult(t *testing.T) {
	wanted := common.HexToHash("0x01")
	address := common.Address{19: 1}
	wrongResult := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Params []map[string]any `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		if len(req.Params) != 1 || req.Params[0]["blockHash"] != wanted.Hex() || req.Params[0]["fromBlock"] != nil || req.Params[0]["toBlock"] != nil {
			t.Errorf("unbound query: %+v", req.Params)
		}
		resultHash := wanted
		if wrongResult {
			resultHash = common.HexToHash("0x02")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": []any{map[string]any{"blockHash": resultHash.Hex(), "blockNumber": "0x1", "transactionHash": common.HexToHash("0x03").Hex(), "transactionIndex": "0x0", "logIndex": "0x0", "address": address.Hex(), "topics": []string{}, "data": "0x", "removed": false}}})
	}))
	defer server.Close()
	c, err := NewHTTPClient(server.URL, time.Second, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	f := Filter{Addresses: []common.Address{address}}
	logs, err := c.LogsByBlockHash(context.Background(), wanted, f)
	if err != nil || len(logs) != 1 || logs[0].BlockHash != wanted {
		t.Fatalf("bound logs %+v %v", logs, err)
	}
	wrongResult = true
	if _, err = c.LogsByBlockHash(context.Background(), wanted, f); !errors.Is(err, ErrMalformed) {
		t.Fatalf("mismatched log accepted: %v", err)
	}
}
func TestTransportErrorRedactsURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	endpoint := server.URL + "/rpc?api_key=marker"
	server.Close()
	c, err := NewHTTPClient(endpoint, time.Second, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.ChainID(context.Background())
	if err == nil || strings.Contains(err.Error(), "marker") {
		t.Fatalf("transport error leaked URL: %v", err)
	}
}
func FuzzParseQuantity(f *testing.F) {
	f.Add("0x0")
	f.Add("0x123")
	f.Add("0x00")
	f.Add("hello")
	f.Fuzz(func(t *testing.T, s string) {
		n, err := parseQuantity(s)
		if err == nil && quantity(n) != s {
			t.Fatalf("noncanonical quantity %q", s)
		}
	})
}
