//go:build crash

package crashharness

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"reorgguard/internal/model"
)

const chainID = 31337

var schemaSerial atomic.Uint64

func h(n byte) common.Hash { var x common.Hash; x[31] = n; return x }
func fixtureBlock(n uint64, id byte, parent common.Hash) model.Block {
	return model.Block{Number: n, Hash: h(id), ParentHash: parent, Time: time.Unix(int64(n+1), 0).UTC()}
}

type rpcFixture struct {
	mu        sync.RWMutex
	active    map[uint64]model.Block
	history   map[common.Hash]model.Block
	logs      map[common.Hash][]model.Log
	head      uint64
	empty     bool
	duplicate bool
	hold      string
	holdFrom  uint64 // zero holds every matching range; nonzero targets one
	arrived   chan string
	release   chan struct{}
	server    *httptest.Server
}

func newRPCFixture(t *testing.T, empty, duplicate bool) *rpcFixture {
	t.Helper()
	f := &rpcFixture{active: map[uint64]model.Block{}, history: map[common.Hash]model.Block{}, logs: map[common.Hash][]model.Log{}, empty: empty, duplicate: duplicate, arrived: make(chan string, 1)}
	g := fixtureBlock(0, 1, common.Hash{})
	f.active[0] = g
	f.history[g.Hash] = g
	f.server = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.server.Close)
	return f
}
func (f *rpcFixture) setBranch(name string, head uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if name != "A" && name != "B" {
		panic("invalid branch")
	}
	f.active = map[uint64]model.Block{0: f.history[h(1)]}
	parent := h(1)
	for n := uint64(1); n <= head; n++ {
		id := byte(n + 1)
		if name == "B" && n >= 3 {
			id = byte(100 + n)
		}
		b := fixtureBlock(n, id, parent)
		f.active[n] = b
		f.history[b.Hash] = b
		if !f.empty && n%2 == 1 {
			f.logs[b.Hash] = []model.Log{{BlockHash: b.Hash, BlockNumber: n, TxHash: h(byte(180 + n)), Index: 0, Address: common.Address{19: 1}, Topics: []common.Hash{h(91)}, Data: []byte{id}}}
		}
		parent = b.Hash
	}
	f.head = head
}
func (f *rpcFixture) holdAt(kind string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hold = kind
	f.release = make(chan struct{})
	f.arrived = make(chan string, 1)
}
func (f *rpcFixture) releaseHold() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.release != nil {
		close(f.release)
		f.release = nil
	}
	f.hold = ""
}
func quantity(n uint64) string { return fmt.Sprintf("0x%x", n) }
func parseQuantity(s string) uint64 {
	n, err := strconv.ParseUint(strings.TrimPrefix(s, "0x"), 16, 64)
	if err != nil {
		panic(err)
	}
	return n
}
func rpcBlockObject(b model.Block) any {
	return map[string]any{"number": quantity(b.Number), "hash": b.Hash.Hex(), "parentHash": b.ParentHash.Hex(), "timestamp": quantity(uint64(b.Time.Unix()))}
}
func rpcLogObject(l model.Log) any {
	topics := make([]string, len(l.Topics))
	for i, v := range l.Topics {
		topics[i] = v.Hex()
	}
	return map[string]any{"blockHash": l.BlockHash.Hex(), "blockNumber": quantity(l.BlockNumber), "transactionHash": l.TxHash.Hex(), "transactionIndex": quantity(uint64(l.TxIndex)), "logIndex": quantity(uint64(l.Index)), "address": l.Address.Hex(), "topics": topics, "data": "0x" + hex.EncodeToString(l.Data), "removed": false}
}
func (f *rpcFixture) serve(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Method string            `json:"method"`
		Params []json.RawMessage `json:"params"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad JSON", 400)
		return
	}
	f.mu.RLock()
	var result any
	var holdKind string
	switch req.Method {
	case "eth_chainId":
		result = quantity(chainID)
	case "eth_blockNumber":
		result = quantity(f.head)
	case "eth_getBlockByNumber":
		var raw string
		_ = json.Unmarshal(req.Params[0], &raw)
		if b, ok := f.active[parseQuantity(raw)]; ok {
			result = rpcBlockObject(b)
		}
	case "eth_getBlockByHash":
		var raw string
		_ = json.Unmarshal(req.Params[0], &raw)
		if b, ok := f.history[common.HexToHash(raw)]; ok {
			result = rpcBlockObject(b)
		}
	case "eth_getLogs":
		var filter map[string]json.RawMessage
		_ = json.Unmarshal(req.Params[0], &filter)
		var logs []model.Log
		if raw, ok := filter["blockHash"]; ok {
			var hs string
			_ = json.Unmarshal(raw, &hs)
			logs = append(logs, f.logs[common.HexToHash(hs)]...)
			holdKind = "branch_logs"
		} else {
			var fromRaw, toRaw string
			_ = json.Unmarshal(filter["fromBlock"], &fromRaw)
			_ = json.Unmarshal(filter["toBlock"], &toRaw)
			for n := parseQuantity(fromRaw); n <= parseQuantity(toRaw); n++ {
				logs = append(logs, f.logs[f.active[n].Hash]...)
			}
			holdKind = "range_logs"
		}
		out := make([]any, 0, len(logs)*2)
		for _, l := range logs {
			out = append(out, rpcLogObject(l))
			if f.duplicate {
				out = append(out, rpcLogObject(l))
			}
		}
		result = out
	default:
		http.Error(w, "unknown method", 400)
		f.mu.RUnlock()
		return
	}
	shouldHold := f.hold == holdKind && holdKind != ""
	if shouldHold && holdKind == "range_logs" && f.holdFrom != 0 {
		var filter map[string]json.RawMessage
		_ = json.Unmarshal(req.Params[0], &filter)
		var fromRaw string
		_ = json.Unmarshal(filter["fromBlock"], &fromRaw)
		shouldHold = parseQuantity(fromRaw) == f.holdFrom
	}
	release := f.release
	arrived := f.arrived
	f.mu.RUnlock()
	if shouldHold {
		select {
		case arrived <- holdKind:
		default:
		}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": result})
}

type probeEvent struct {
	point string
	conn  net.Conn
}
type probeController struct {
	ln     net.Listener
	events chan probeEvent
	done   chan struct{}
}

func newController(t *testing.T) *probeController {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	c := &probeController{ln: ln, events: make(chan probeEvent, 2), done: make(chan struct{})}
	go func() {
		defer close(c.done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				line, readErr := bufio.NewReader(conn).ReadString('\n')
				if readErr != nil {
					conn.Close()
					return
				}
				c.events <- probeEvent{strings.TrimSpace(line), conn}
			}()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		<-c.done
		for {
			select {
			case ev := <-c.events:
				_ = ev.conn.Close()
			default:
				return
			}
		}
	})
	return c
}

type child struct {
	cmd    *exec.Cmd
	done   chan error
	reaped chan struct{}
	output bytes.Buffer
}

func newChild(cmd *exec.Cmd) *child {
	return &child{cmd: cmd, done: make(chan error, 1), reaped: make(chan struct{})}
}

func (p *child) reap() {
	err := p.cmd.Wait()
	close(p.reaped)
	p.done <- err
}

func startChild(t *testing.T, dsn string, f *rpcFixture, c *probeController, point string) *child {
	t.Helper()
	binary := os.Getenv("REORGGUARD_CRASH_BINARY")
	if binary == "" {
		t.Fatal("REORGGUARD_CRASH_BINARY required")
	}
	p := newChild(exec.Command(binary))
	p.cmd.Env = append(os.Environ(), "REORGGUARD_RPC_HTTP_URL="+f.server.URL, "REORGGUARD_DATABASE_URL="+dsn, "REORGGUARD_CHAIN_ID=31337", "REORGGUARD_GENESIS_HASH="+h(1).Hex(), "REORGGUARD_START_BLOCK=1", "REORGGUARD_ADDRESS="+(common.Address{19: 1}).Hex(), "REORGGUARD_MAX_REORG_DEPTH=4")
	if point != "" {
		p.cmd.Env = append(p.cmd.Env, "REORGGUARD_CRASHPROBE_POINT="+point, "REORGGUARD_CRASHPROBE_ADDR="+c.ln.Addr().String())
	}
	p.cmd.Stdout = &p.output
	p.cmd.Stderr = &p.output
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
				t.Error("child process did not reap")
			}
		}
	})
	return p
}

func TestChildReapSignalPrecedesCompletionDelivery(t *testing.T) {
	p := newChild(exec.Command(os.Args[0], "-test.run=^$"))
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go p.reap()
	if err := <-p.done; err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.reaped:
	default:
		t.Fatal("completion delivered before process reap was observable")
	}
}
func (p *child) wait(t *testing.T, wantSuccess bool) {
	t.Helper()
	select {
	case err := <-p.done:
		if wantSuccess && err != nil {
			t.Fatalf("child failed: %v\n%s", err, p.output.String())
		}
		if !wantSuccess && err == nil {
			t.Fatalf("child unexpectedly succeeded:\n%s", p.output.String())
		}
	case <-time.After(30 * time.Second):
		_ = p.cmd.Process.Kill()
		t.Fatalf("child timed out: %s", p.output.String())
	}
}
func (p *child) kill(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	p.wait(t, false)
}
func (p *child) term(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-p.done:
		if err != nil {
			t.Fatalf("SIGTERM did not exit gracefully: %v\n%s", err, p.output.String())
		}
	case <-time.After(6 * time.Second):
		_ = p.cmd.Process.Kill()
		t.Fatal("SIGTERM exceeded five-second shutdown deadline")
	}
}
func waitProbe(t *testing.T, c *probeController, point string) probeEvent {
	t.Helper()
	select {
	case ev := <-c.events:
		if ev.point != point {
			t.Fatalf("probe %s, want %s", ev.point, point)
		}
		return ev
	case <-time.After(12 * time.Second):
		t.Fatalf("probe %s not reached", point)
		return probeEvent{}
	}
}
func waitRPC(t *testing.T, f *rpcFixture, kind string) {
	t.Helper()
	select {
	case got := <-f.arrived:
		if got != kind {
			t.Fatalf("RPC %s, want %s", got, kind)
		}
	case <-time.After(12 * time.Second):
		t.Fatalf("RPC %s not reached", kind)
	}
}

func schemaDSN(t *testing.T) string {
	t.Helper()
	base := os.Getenv("REORGGUARD_TEST_DATABASE_URL")
	if base == "" {
		t.Fatal("REORGGUARD_TEST_DATABASE_URL required")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("rg_crash_%d_%d", time.Now().UnixNano(), schemaSerial.Add(1))
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		admin.Close(context.Background())
	})
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return u.String()
}

type snapshot struct {
	cpNumber        int64
	cpHash          common.Hash
	canonicalBlocks int
	canonicalLogs   int
	allBlocks       int
	allLogs         int
	orphanBlocks    int
	checksum        string
}

func appendRecord(dst hash.Hash, parts ...any) {
	for _, p := range parts {
		fmt.Fprintf(dst, "%v|", p)
	}
	fmt.Fprintln(dst)
}
func inspect(t *testing.T, dsn string) snapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	var snap snapshot
	var cpHash []byte
	if err = conn.QueryRow(ctx, "SELECT checkpoint_number,checkpoint_hash FROM sync_state WHERE chain_id=31337").Scan(&snap.cpNumber, &cpHash); err != nil {
		t.Fatal(err)
	}
	snap.cpHash = common.BytesToHash(cpHash)
	rows, err := conn.Query(ctx, `SELECT number,hash,parent_hash,block_time,canonical,logs_complete,log_count,log_set_hash FROM blocks WHERE chain_id=31337 ORDER BY number,hash`)
	if err != nil {
		t.Fatal(err)
	}
	type record struct {
		n            int64
		hash, parent common.Hash
		canonical    bool
		count        int64
		digest       [32]byte
	}
	blocks := map[common.Hash]record{}
	canonical := map[int64]record{}
	checksum := sha256.New()
	for rows.Next() {
		var n int64
		var blockTime time.Time
		var bh, ph, lh []byte
		var isCanonical, complete bool
		var count *int64
		if err = rows.Scan(&n, &bh, &ph, &blockTime, &isCanonical, &complete, &count, &lh); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		snap.allBlocks++
		if !isCanonical {
			snap.orphanBlocks++
		}
		if n > 0 && (!complete || count == nil || len(lh) != 32) {
			rows.Close()
			t.Fatalf("incomplete historical block %d", n)
		}
		rec := record{n: n, hash: common.BytesToHash(bh), parent: common.BytesToHash(ph), canonical: isCanonical}
		if count != nil {
			rec.count = *count
		}
		copy(rec.digest[:], lh)
		blocks[rec.hash] = rec
		if isCanonical {
			if _, ok := canonical[n]; ok {
				rows.Close()
				t.Fatalf("two canonical blocks at height %d", n)
			}
			canonical[n] = rec
			snap.canonicalBlocks++
			appendRecord(checksum, "B", n, hex.EncodeToString(bh), hex.EncodeToString(ph), blockTime.UnixNano(), rec.count, hex.EncodeToString(lh))
		}
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	rows.Close()
	if snap.canonicalBlocks != int(snap.cpNumber+1) {
		t.Fatalf("canonical gap: cp=%d blocks=%d", snap.cpNumber, snap.canonicalBlocks)
	}
	for n := int64(0); n <= snap.cpNumber; n++ {
		b, ok := canonical[n]
		if !ok {
			t.Fatalf("missing canonical height %d", n)
		}
		if n > 0 && b.parent != canonical[n-1].hash {
			t.Fatalf("broken canonical parent %d", n)
		}
	}
	for _, b := range blocks {
		if b.n == 0 {
			continue
		}
		parent, ok := blocks[b.parent]
		if !ok || parent.n+1 != b.n {
			t.Fatalf("historical parent missing or wrong at %d/%s", b.n, b.hash)
		}
	}
	if canonical[snap.cpNumber].hash != snap.cpHash {
		t.Fatalf("checkpoint not canonical: %d/%s", snap.cpNumber, snap.cpHash)
	}
	logRows, err := conn.Query(ctx, `SELECT block_hash,block_number,tx_hash,tx_index,log_index,address,topics,data FROM logs WHERE chain_id=31337 ORDER BY block_number,block_hash,log_index`)
	if err != nil {
		t.Fatal(err)
	}
	byBlock := map[common.Hash][]model.Log{}
	seen := map[string]bool{}
	for logRows.Next() {
		var bh, txh, addr, data []byte
		var n int64
		var txi, li int32
		var topics [][]byte
		if err = logRows.Scan(&bh, &n, &txh, &txi, &li, &addr, &topics, &data); err != nil {
			logRows.Close()
			t.Fatal(err)
		}
		snap.allLogs++
		blockHash := common.BytesToHash(bh)
		b, ok := blocks[blockHash]
		if !ok || b.n != n {
			logRows.Close()
			t.Fatalf("log with missing/wrong block %d", n)
		}
		key := fmt.Sprintf("%x:%d", bh, li)
		if seen[key] {
			logRows.Close()
			t.Fatalf("duplicate log %s", key)
		}
		seen[key] = true
		l := model.Log{BlockHash: blockHash, BlockNumber: uint64(n), TxHash: common.BytesToHash(txh), TxIndex: uint32(txi), Index: uint32(li), Address: common.BytesToAddress(addr), Data: data}
		for _, tp := range topics {
			l.Topics = append(l.Topics, common.BytesToHash(tp))
		}
		byBlock[blockHash] = append(byBlock[blockHash], l)
		if b.canonical {
			snap.canonicalLogs++
			fields := []any{"L", n, hex.EncodeToString(bh), hex.EncodeToString(txh), txi, li, hex.EncodeToString(addr), len(topics)}
			for _, topic := range topics {
				fields = append(fields, hex.EncodeToString(topic))
			}
			fields = append(fields, hex.EncodeToString(data))
			appendRecord(checksum, fields...)
		}
	}
	if err = logRows.Err(); err != nil {
		logRows.Close()
		t.Fatal(err)
	}
	logRows.Close()
	for _, b := range blocks {
		if b.n == 0 {
			continue
		}
		logs := byBlock[b.hash]
		if int64(len(logs)) != b.count || model.FingerprintLogs(logs) != b.digest {
			t.Fatalf("historical log set corrupt at %d/%s", b.n, b.hash)
		}
	}
	snap.checksum = hex.EncodeToString(checksum.Sum(nil))
	return snap
}
func compare(t *testing.T, actual, reference snapshot) {
	t.Helper()
	if actual.cpNumber != reference.cpNumber || actual.cpHash != reference.cpHash || actual.canonicalBlocks != reference.canonicalBlocks || actual.canonicalLogs != reference.canonicalLogs || actual.checksum != reference.checksum || actual.allBlocks != reference.allBlocks || actual.allLogs != reference.allLogs || actual.orphanBlocks != reference.orphanBlocks {
		t.Fatalf("snapshot mismatch\nactual %+v\nreference %+v", actual, reference)
	}
}
func evidence(t *testing.T, name, boundary string, pre, post, ref snapshot) {
	t.Helper()
	status := "PASS"
	if post.checksum != ref.checksum {
		status = "FAIL"
	}
	t.Logf("EVIDENCE scenario=%s boundary=%s pre_checkpoint=%d/%s post_checkpoint=%d/%s expected_head=%d/%s actual_head=%d/%s canonical_blocks=%d canonical_logs=%d all_blocks=%d all_logs=%d orphan_blocks=%d checksum=%s result=%s", name, boundary, pre.cpNumber, pre.cpHash.Hex(), post.cpNumber, post.cpHash.Hex(), ref.cpNumber, ref.cpHash.Hex(), post.cpNumber, post.cpHash.Hex(), post.canonicalBlocks, post.canonicalLogs, post.allBlocks, post.allLogs, post.orphanBlocks, post.checksum, status)
}
func runSuccess(t *testing.T, dsn string, f *rpcFixture) {
	t.Helper()
	p := startChild(t, dsn, f, nil, "")
	p.wait(t, true)
}
func referenceRun(t *testing.T, f *rpcFixture, stages []string, head uint64) snapshot {
	t.Helper()
	dsn := schemaDSN(t)
	for _, stage := range stages {
		f.setBranch(stage, head)
		runSuccess(t, dsn, f)
	}
	return inspect(t, dsn)
}
