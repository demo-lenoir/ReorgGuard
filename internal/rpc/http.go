// Package rpc implements the bounded HTTP JSON-RPC subset needed by historical backfill.
package rpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"go.opentelemetry.io/otel/attribute"
	"reorgguard/internal/model"
	"reorgguard/internal/telemetry"
)

var ErrResponseTooLarge = errors.New("RPC response too large")
var ErrMalformed = errors.New("malformed RPC response")
var ErrNotFound = errors.New("RPC block not found")
var ErrRateLimited = errors.New("RPC endpoint rate limited")

// RPCError retains provider fields for classification, but Error never prints
// provider-controlled text that could contain a credential-bearing URL.
type RPCError struct {
	Code    int
	Message string
	Data    json.RawMessage
}

func (e *RPCError) Error() string { return fmt.Sprintf("RPC error code %d", e.Code) }

type Filter struct {
	Addresses []common.Address
	Topics    [][]common.Hash
}

// Matches checks Ethereum's address OR-set and positional topic OR-sets.
// An empty topic group is a wildcard, but positions after it still require a
// topic at their own index.
func (f Filter) Matches(log model.Log) bool {
	if len(f.Addresses) > 0 {
		matched := false
		for _, address := range f.Addresses {
			matched = matched || address == log.Address
		}
		if !matched {
			return false
		}
	}
	if len(log.Topics) < len(f.Topics) {
		return false
	}
	for position, choices := range f.Topics {
		if len(choices) == 0 {
			continue
		}
		matched := false
		for _, choice := range choices {
			matched = matched || choice == log.Topics[position]
		}
		if !matched {
			return false
		}
	}
	return true
}

func (f Filter) ValidateLogs(logs []model.Log) error {
	for _, log := range logs {
		if !f.Matches(log) {
			return ErrMalformed
		}
	}
	return nil
}

func (f Filter) Validate() error {
	if len(f.Addresses) > 16 || len(f.Topics) > 4 {
		return errors.New("filter exceeds configured shape")
	}
	selective := len(f.Addresses) > 0
	for _, choices := range f.Topics {
		if len(choices) > 16 {
			return errors.New("topic OR-set exceeds limit")
		}
		if len(choices) > 0 {
			selective = true
		}
	}
	if !selective {
		return errors.New("filter must select an address or topic")
	}
	return nil
}

func (f Filter) Fingerprint() [32]byte {
	addresses := make([]string, len(f.Addresses))
	for i, a := range f.Addresses {
		addresses[i] = a.Hex()
	}
	sort.Strings(addresses)
	topics := make([][]string, len(f.Topics))
	for i, group := range f.Topics {
		for _, t := range group {
			topics[i] = append(topics[i], t.Hex())
		}
		sort.Strings(topics[i])
	}
	encoded, _ := json.Marshal(struct {
		Addresses []string   `json:"addresses"`
		Topics    [][]string `json:"topics"`
	}{addresses, topics})
	return sha256.Sum256(encoded)
}

func (f Filter) rpcObject(from, to uint64) map[string]any {
	obj := map[string]any{"fromBlock": quantity(from), "toBlock": quantity(to)}
	if len(f.Addresses) == 1 {
		obj["address"] = f.Addresses[0].Hex()
	} else if len(f.Addresses) > 1 {
		addresses := make([]string, len(f.Addresses))
		for i, a := range f.Addresses {
			addresses[i] = a.Hex()
		}
		obj["address"] = addresses
	}
	if len(f.Topics) > 0 {
		groups := make([]any, len(f.Topics))
		for i, group := range f.Topics {
			if len(group) == 0 {
				groups[i] = nil
			} else if len(group) == 1 {
				groups[i] = group[0].Hex()
			} else {
				choices := make([]string, len(group))
				for j, t := range group {
					choices[j] = t.Hex()
				}
				groups[i] = choices
			}
		}
		obj["topics"] = groups
	}
	return obj
}

func (f Filter) blockObject(hash common.Hash) map[string]any {
	obj := f.rpcObject(0, 0)
	delete(obj, "fromBlock")
	delete(obj, "toBlock")
	obj["blockHash"] = hash.Hex()
	return obj
}

func (f Filter) subscriptionObject() map[string]any {
	obj := f.rpcObject(0, 0)
	delete(obj, "fromBlock")
	delete(obj, "toBlock")
	return obj
}

type HTTPClient struct {
	endpoint         string
	client           *http.Client
	maxResponseBytes int64
	metricsMu        sync.Mutex
	requests         [6][2]uint64
	latency          [6]time.Duration
}

var rpcMethods = [...]string{"eth_chainId", "eth_blockNumber", "eth_getBlockByNumber", "eth_getBlockByHash", "eth_getLogs", "eth_getLogsByHash"}

// WritePrometheus emits fixed method/status series for a single-endpoint setup.
// Endpoint is supplied by the caller as a configured logical name, never a URL.
func (c *HTTPClient) WritePrometheus(w io.Writer, endpoint string) error {
	if endpoint != "primary" {
		return errors.New("invalid metric endpoint")
	}
	c.metricsMu.Lock()
	defer c.metricsMu.Unlock()
	for i, method := range rpcMethods {
		for j, status := range []string{"ok", "error"} {
			if _, err := fmt.Fprintf(w, "reorgguard_rpc_requests_total{endpoint=%q,method=%q,status=%q} %d\n", endpoint, method, status, c.requests[i][j]); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintf(w, "reorgguard_rpc_latency_seconds{endpoint=%q,method=%q} %g\n", endpoint, method, c.latency[i].Seconds()); err != nil {
			return err
		}
	}
	return nil
}

func NewHTTPClient(endpoint string, timeout time.Duration, maxResponseBytes int64) (*HTTPClient, error) {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || timeout <= 0 || maxResponseBytes < 1024 {
		return nil, errors.New("invalid HTTP RPC configuration")
	}
	return &HTTPClient{endpoint: endpoint, client: &http.Client{Timeout: timeout}, maxResponseBytes: maxResponseBytes}, nil
}

func (c *HTTPClient) call(ctx context.Context, method string, params any, out any) (retErr error) {
	started := time.Now()
	ctx, span := telemetry.Start(ctx, "rpc.read", attribute.String("rpc.method", method))
	defer span.End()
	defer func() {
		for i, known := range rpcMethods {
			if method == known {
				result := 0
				if retErr != nil {
					result = 1
				}
				c.metricsMu.Lock()
				c.requests[i][result]++
				c.latency[i] = time.Since(started)
				c.metricsMu.Unlock()
				break
			}
		}
	}()
	defer func() {
		if retErr != nil {
			category := "rpc_error"
			if errors.Is(retErr, ErrRateLimited) {
				category = "rate_limited"
			}
			if errors.Is(retErr, context.DeadlineExceeded) {
				category = "timeout"
			}
			telemetry.Failure(span, category)
		}
	}()
	requestBody, err := json.Marshal(struct {
		JSONRPC string `json:"jsonrpc"`
		ID      int    `json:"id"`
		Method  string `json:"method"`
		Params  any    `json:"params"`
	}{"2.0", 1, method, params})
	if err != nil {
		return fmt.Errorf("encode %s: %w", method, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(requestBody))
	if err != nil {
		return fmt.Errorf("build %s request: %w", method, ErrMalformed)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = urlErr.Err
		}
		return fmt.Errorf("%s transport: %w", method, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusTooManyRequests {
			return ErrRateLimited
		}
		return fmt.Errorf("%s HTTP status %d", method, resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, c.maxResponseBytes+1))
	if err != nil {
		return fmt.Errorf("read %s response: %w", method, err)
	}
	if int64(len(data)) > c.maxResponseBytes {
		return ErrResponseTooLarge
	}
	var envelope struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  json.RawMessage `json:"result"`
		Error   *RPCError       `json:"error"`
	}
	if err = json.Unmarshal(data, &envelope); err != nil || envelope.JSONRPC != "2.0" || string(envelope.ID) != "1" {
		return ErrMalformed
	}
	if envelope.Error != nil {
		return envelope.Error
	}
	if len(envelope.Result) == 0 {
		return ErrMalformed
	}
	if bytes.Equal(bytes.TrimSpace(envelope.Result), []byte("null")) {
		return ErrNotFound
	}
	if err = json.Unmarshal(envelope.Result, out); err != nil {
		return fmt.Errorf("decode %s result: %w", method, ErrMalformed)
	}
	return nil
}

func (c *HTTPClient) ChainID(ctx context.Context) (uint64, error) {
	var raw string
	if err := c.call(ctx, "eth_chainId", []any{}, &raw); err != nil {
		return 0, err
	}
	return parseQuantity(raw)
}
func (c *HTTPClient) BlockNumber(ctx context.Context) (uint64, error) {
	var raw string
	if err := c.call(ctx, "eth_blockNumber", []any{}, &raw); err != nil {
		return 0, err
	}
	return parseQuantity(raw)
}
func (c *HTTPClient) BlockByNumber(ctx context.Context, n uint64) (model.Block, error) {
	var raw *rpcBlock
	if err := c.call(ctx, "eth_getBlockByNumber", []any{quantity(n), false}, &raw); err != nil {
		return model.Block{}, err
	}
	if raw == nil {
		return model.Block{}, ErrNotFound
	}
	b, err := raw.parse()
	if err != nil {
		return model.Block{}, err
	}
	if b.Number != n {
		return model.Block{}, ErrMalformed
	}
	return b, nil
}
func (c *HTTPClient) BlockByHash(ctx context.Context, h common.Hash) (model.Block, error) {
	var raw *rpcBlock
	if err := c.call(ctx, "eth_getBlockByHash", []any{h.Hex(), false}, &raw); err != nil {
		return model.Block{}, err
	}
	if raw == nil {
		return model.Block{}, ErrNotFound
	}
	b, err := raw.parse()
	if err != nil {
		return model.Block{}, err
	}
	if b.Hash != h {
		return model.Block{}, ErrMalformed
	}
	return b, nil
}
func (c *HTTPClient) Logs(ctx context.Context, from, to uint64, f Filter) ([]model.Log, error) {
	if err := f.Validate(); err != nil {
		return nil, err
	}
	return c.getLogs(ctx, f.rpcObject(from, to), f)
}

// LogsByBlockHash binds replacement-branch logs to one immutable header.
func (c *HTTPClient) LogsByBlockHash(ctx context.Context, hash common.Hash, f Filter) ([]model.Log, error) {
	if hash == (common.Hash{}) {
		return nil, ErrMalformed
	}
	if err := f.Validate(); err != nil {
		return nil, err
	}
	logs, err := c.getLogs(ctx, f.blockObject(hash), f)
	if err != nil {
		return nil, err
	}
	for _, l := range logs {
		if l.BlockHash != hash {
			return nil, ErrMalformed
		}
	}
	return logs, nil
}

func (c *HTTPClient) getLogs(ctx context.Context, object map[string]any, filter Filter) ([]model.Log, error) {
	var raw []rpcLog
	if err := c.call(ctx, "eth_getLogs", []any{object}, &raw); err != nil {
		return nil, err
	}
	logs := make([]model.Log, len(raw))
	for i, r := range raw {
		l, err := r.parse()
		if err != nil {
			return nil, fmt.Errorf("log %d: %w", i, err)
		}
		logs[i] = l
	}
	if err := filter.ValidateLogs(logs); err != nil {
		return nil, err
	}
	return logs, nil
}

type rpcBlock struct {
	Number     string `json:"number"`
	Hash       string `json:"hash"`
	ParentHash string `json:"parentHash"`
	Timestamp  string `json:"timestamp"`
}

func (r rpcBlock) parse() (model.Block, error) {
	n, err := parseQuantity(r.Number)
	if err != nil {
		return model.Block{}, err
	}
	h, err := parseHash(r.Hash)
	if err != nil {
		return model.Block{}, err
	}
	p, err := parseHash(r.ParentHash)
	if err != nil {
		return model.Block{}, err
	}
	ts, err := parseQuantity(r.Timestamp)
	if err != nil || ts > uint64(^uint64(0)>>1) {
		return model.Block{}, ErrMalformed
	}
	return model.Block{Number: n, Hash: h, ParentHash: p, Time: time.Unix(int64(ts), 0).UTC()}, nil
}

type rpcLog struct {
	BlockHash        string   `json:"blockHash"`
	BlockNumber      string   `json:"blockNumber"`
	TransactionHash  string   `json:"transactionHash"`
	TransactionIndex string   `json:"transactionIndex"`
	LogIndex         string   `json:"logIndex"`
	Address          string   `json:"address"`
	Topics           []string `json:"topics"`
	Data             string   `json:"data"`
	Removed          bool     `json:"removed"`
}

func (r rpcLog) parse() (model.Log, error) {
	if r.Removed || r.Topics == nil {
		return model.Log{}, ErrMalformed
	}
	bh, err := parseHash(r.BlockHash)
	if err != nil {
		return model.Log{}, err
	}
	bn, err := parseQuantity(r.BlockNumber)
	if err != nil {
		return model.Log{}, err
	}
	th, err := parseHash(r.TransactionHash)
	if err != nil {
		return model.Log{}, err
	}
	ti, err := parseQuantity(r.TransactionIndex)
	if err != nil || ti > uint64(^uint32(0)) {
		return model.Log{}, ErrMalformed
	}
	li, err := parseQuantity(r.LogIndex)
	if err != nil || li > uint64(^uint32(0)) {
		return model.Log{}, ErrMalformed
	}
	a, err := parseAddress(r.Address)
	if err != nil {
		return model.Log{}, err
	}
	if len(r.Topics) > 4 {
		return model.Log{}, ErrMalformed
	}
	topics := make([]common.Hash, len(r.Topics))
	for i, s := range r.Topics {
		topics[i], err = parseHash(s)
		if err != nil {
			return model.Log{}, err
		}
	}
	data, err := parseBytes(r.Data)
	if err != nil {
		return model.Log{}, err
	}
	return model.Log{BlockHash: bh, BlockNumber: bn, TxHash: th, TxIndex: uint32(ti), Index: uint32(li), Address: a, Topics: topics, Data: data}, nil
}

func quantity(n uint64) string { return fmt.Sprintf("0x%x", n) }
func parseQuantity(s string) (uint64, error) {
	if !strings.HasPrefix(s, "0x") || len(s) < 3 || (len(s) > 3 && s[2] == '0') || strings.ToLower(s) != s {
		return 0, ErrMalformed
	}
	n, err := strconv.ParseUint(s[2:], 16, 64)
	if err != nil {
		return 0, ErrMalformed
	}
	return n, nil
}
func parseBytes(s string) ([]byte, error) {
	if !strings.HasPrefix(s, "0x") || len(s)%2 != 0 {
		return nil, ErrMalformed
	}
	b, err := hex.DecodeString(s[2:])
	if err != nil {
		return nil, ErrMalformed
	}
	return b, nil
}
func parseHash(s string) (common.Hash, error) {
	b, err := parseBytes(s)
	if err != nil || len(b) != 32 {
		return common.Hash{}, ErrMalformed
	}
	return common.BytesToHash(b), nil
}
func parseAddress(s string) (common.Address, error) {
	b, err := parseBytes(s)
	if err != nil || len(b) != 20 {
		return common.Address{}, ErrMalformed
	}
	return common.BytesToAddress(b), nil
}

// IsRangeLimit classifies explicit provider codes and structured reasons.
// A narrow allowlist of known text forms is used only for generic provider codes.
func IsRangeLimit(err error) bool {
	if errors.Is(err, ErrResponseTooLarge) {
		return true
	}
	var rpcErr *RPCError
	if !errors.As(err, &rpcErr) {
		return false
	}
	if rpcErr.Code == -32005 {
		return true
	}
	var data struct {
		Reason        string `json:"reason"`
		LimitExceeded bool   `json:"limitExceeded"`
	}
	if json.Unmarshal(rpcErr.Data, &data) == nil && (data.LimitExceeded || data.Reason == "too_many_results" || data.Reason == "range_limit") {
		return true
	}
	if rpcErr.Code == -32602 || rpcErr.Code == -32000 {
		message := strings.ToLower(strings.TrimSpace(rpcErr.Message))
		return message == "too many results" || strings.HasPrefix(message, "query returned more than ") || strings.HasPrefix(message, "block range too large")
	}
	return false
}
