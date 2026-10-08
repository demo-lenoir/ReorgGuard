package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// A provider is allowed to return an error, but it is never allowed to widen
// the operator's persisted filter by returning additional successful rows.
func TestHTTPLogResponsesRespectLocalFilter(t *testing.T) {
	address := common.Address{19: 1}
	otherAddress := common.Address{19: 2}
	a, b, c := common.Hash{31: 1}, common.Hash{31: 2}, common.Hash{31: 3}
	filter := Filter{Addresses: []common.Address{address}, Topics: [][]common.Hash{{a, b}, {}, {c}}}
	row := func(addr common.Address, topics ...common.Hash) map[string]any {
		strings := make([]string, len(topics))
		for i, topic := range topics {
			strings[i] = topic.Hex()
		}
		return map[string]any{"blockHash": common.Hash{31: 9}.Hex(), "blockNumber": "0x1", "transactionHash": common.Hash{31: 8}.Hex(), "transactionIndex": "0x0", "logIndex": "0x0", "address": addr.Hex(), "topics": strings, "data": "0x", "removed": false}
	}
	valid := row(address, b, a, c)
	cases := []struct {
		name   string
		rows   []any
		byHash bool
		ok     bool
	}{
		{"alternative_and_wildcard", []any{valid}, false, true},
		{"wrong_address", []any{row(otherAddress, b, a, c)}, false, false},
		{"wrong_topic0", []any{row(address, c, a, c)}, false, false},
		{"wrong_later_topic", []any{row(address, a, b, a)}, false, false},
		{"missing_required_topic", []any{row(address, a, b)}, false, false},
		{"mixed_valid_and_invalid", []any{valid, row(otherAddress, a, b, c)}, false, false},
		{"block_hash_wrong_filter", []any{row(otherAddress, a, b, c)}, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": tc.rows})
			}))
			defer server.Close()
			client, err := NewHTTPClient(server.URL, time.Second, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			var logs []any
			if tc.byHash {
				parsed, callErr := client.LogsByBlockHash(context.Background(), common.Hash{31: 9}, filter)
				logs, err = make([]any, len(parsed)), callErr
			} else {
				parsed, callErr := client.Logs(context.Background(), 1, 1, filter)
				logs, err = make([]any, len(parsed)), callErr
			}
			if tc.ok {
				if err != nil || len(logs) != 1 {
					t.Fatalf("valid response: rows=%d err=%v", len(logs), err)
				}
			} else if !errors.Is(err, ErrMalformed) || len(logs) != 0 {
				t.Fatalf("out-of-filter response accepted: rows=%d err=%v", len(logs), err)
			}
		})
	}
}
