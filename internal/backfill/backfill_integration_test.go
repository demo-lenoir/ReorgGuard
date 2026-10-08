//go:build integration

package backfill

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"reorgguard/internal/rpc"
	"reorgguard/internal/store"
)

func TestPostgresBackfillRestartFromCheckpointPlusOne(t *testing.T) {
	dsn := os.Getenv("REORGGUARD_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("REORGGUARD_TEST_DATABASE_URL required")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("rg_runner_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); admin.Close(ctx) }()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	r := newRunner(4)
	fixtureBlocks := r.Source.(*source).blocks
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var call struct {
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		if err := json.NewDecoder(req.Body).Decode(&call); err != nil {
			t.Error(err)
			return
		}
		var result any
		switch call.Method {
		case "eth_chainId":
			result = "0x7a69"
		case "eth_blockNumber":
			result = "0x4"
		case "eth_getBlockByNumber", "eth_getBlockByHash":
			var q string
			if err := json.Unmarshal(call.Params[0], &q); err != nil {
				t.Error(err)
				return
			}
			var n uint64
			var err error
			if call.Method == "eth_getBlockByHash" {
				for i, block := range fixtureBlocks {
					if block.Hash.Hex() == q {
						n = uint64(i)
						break
					}
				}
			} else {
				n, err = strconv.ParseUint(q[2:], 16, 64)
			}
			if err != nil || n >= uint64(len(fixtureBlocks)) {
				t.Errorf("bad block request %s", q)
				return
			}
			b := fixtureBlocks[n]
			result = map[string]any{"number": fmt.Sprintf("0x%x", b.Number), "hash": b.Hash.Hex(), "parentHash": b.ParentHash.Hex(), "timestamp": fmt.Sprintf("0x%x", b.Time.Unix())}
		case "eth_getLogs":
			var filter map[string]string
			if err := json.Unmarshal(call.Params[0], &filter); err != nil {
				t.Error(err)
				return
			}
			var from, to uint64
			if hash := filter["blockHash"]; hash != "" {
				for i, block := range fixtureBlocks {
					if block.Hash.Hex() == hash {
						from, to = uint64(i), uint64(i)
						break
					}
				}
			} else {
				from, _ = strconv.ParseUint(filter["fromBlock"][2:], 16, 64)
				to, _ = strconv.ParseUint(filter["toBlock"][2:], 16, 64)
			}
			result = []any{}
			if from <= 1 && to >= 1 {
				result = []any{map[string]any{"blockHash": fixtureBlocks[1].Hash.Hex(), "blockNumber": "0x1", "transactionHash": h(90).Hex(), "transactionIndex": "0x0", "logIndex": "0x0", "address": common.Address{19: 1}.Hex(), "topics": []string{h(91).Hex()}, "data": "0x01", "removed": false}}
			}
		default:
			t.Errorf("unexpected method %s", call.Method)
			return
		}
		if err := json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": result}); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	client, err := rpc.NewHTTPClient(server.URL, time.Second, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	r.Source = client
	repo, err := store.OpenConfig(ctx, cfg, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	r.Store = repo
	first, err := r.RunTo(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if first.Checkpoint.Number != 2 {
		t.Fatalf("first checkpoint: %+v", first)
	}
	repo.Close()
	reopened, err := store.OpenConfig(ctx, cfg, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	r.Store = reopened
	var requested [][2]uint64
	r.OnRange = func(from, to uint64, _ int, _ uint64) { requested = append(requested, [2]uint64{from, to}) }
	second, err := r.RunTo(ctx, 4)
	if err != nil {
		t.Fatal(err)
	}
	if second.Checkpoint.Number != 4 || !reflect.DeepEqual(requested, [][2]uint64{{3, 4}}) {
		t.Fatalf("resume %+v ranges %v", second, requested)
	}
	queryDB, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer queryDB.Close()
	var blocks int
	if err = queryDB.QueryRow(ctx, "SELECT count(*) FROM blocks WHERE canonical").Scan(&blocks); err != nil {
		t.Fatal(err)
	}
	if blocks != 5 {
		t.Fatalf("gap/duplicate: %d canonical blocks", blocks)
	}
	var logs int
	if err = queryDB.QueryRow(ctx, "SELECT count(*) FROM logs").Scan(&logs); err != nil {
		t.Fatal(err)
	}
	if logs != 1 {
		t.Fatalf("HTTP log not persisted exactly once: %d", logs)
	}
}
