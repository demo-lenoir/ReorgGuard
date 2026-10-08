package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gorilla/websocket"
)

var ErrWSWrongChain = errors.New("WebSocket chain ID or genesis mismatch")
var ErrWSMalformed = errors.New("malformed WebSocket RPC message")
var ErrWSTransport = errors.New("WebSocket transport failure")

type HintKind uint8

const (
	HeadHint HintKind = iota + 1
	LogHint
)

// Hint contains untrusted notification identity. It is never written to SQL.
type Hint struct {
	Kind    HintKind
	Number  uint64
	Hash    common.Hash
	Removed bool
}

type WSClient struct {
	endpoint    string
	ChainID     uint64
	Genesis     common.Hash
	Filter      Filter
	DialTimeout time.Duration
	Heartbeat   time.Duration
	MaxMessage  int64
}

func NewWSClient(endpoint string, chainID uint64, genesis common.Hash, filter Filter) (*WSClient, error) {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "ws" && u.Scheme != "wss") || u.Host == "" || chainID == 0 || genesis == (common.Hash{}) || filter.Validate() != nil {
		return nil, errors.New("invalid WebSocket RPC configuration")
	}
	return &WSClient{endpoint: endpoint, ChainID: chainID, Genesis: genesis, Filter: filter,
		DialTimeout: 10 * time.Second, Heartbeat: 30 * time.Second, MaxMessage: 1 << 20}, nil
}

// Subscribe owns exactly one connection and its heartbeat. onConnected is
// called only after chain/anchor verification and both subscriptions succeed.
// The caller owns reconnect and must treat all delivered hints as untrusted.
func (c *WSClient) Subscribe(ctx context.Context, onConnected func(), onHint func(Hint)) error {
	if c == nil || c.DialTimeout <= 0 || c.Heartbeat <= 0 || c.MaxMessage < 1024 || onHint == nil {
		return errors.New("invalid WebSocket subscription configuration")
	}
	dialer := websocket.Dialer{HandshakeTimeout: c.DialTimeout}
	conn, _, err := dialer.DialContext(ctx, c.endpoint, nil)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return ErrWSTransport // never expose URL/userinfo/query from dial errors
	}
	defer conn.Close()
	stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopClose()
	conn.SetReadLimit(c.MaxMessage)
	if err := conn.SetReadDeadline(time.Now().Add(2 * c.Heartbeat)); err != nil {
		return ErrWSTransport
	}
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(2 * c.Heartbeat)) })
	var chainRaw string
	if err := c.call(conn, 1, "eth_chainId", []any{}, &chainRaw); err != nil {
		return err
	}
	chain, err := parseQuantity(chainRaw)
	if err != nil {
		return ErrWSMalformed
	}
	if chain != c.ChainID {
		return ErrWSWrongChain
	}
	var genesisRaw rpcBlock
	if err := c.call(conn, 2, "eth_getBlockByNumber", []any{"0x0", false}, &genesisRaw); err != nil {
		return err
	}
	genesis, err := genesisRaw.parse()
	if err != nil || genesis.Number != 0 {
		return ErrWSMalformed
	}
	if genesis.Hash != c.Genesis {
		return ErrWSWrongChain
	}
	var headsID string
	if err := c.call(conn, 3, "eth_subscribe", []any{"newHeads"}, &headsID); err != nil {
		return err
	}
	var logsID string
	if err := c.call(conn, 4, "eth_subscribe", []any{"logs", c.Filter.subscriptionObject()}, &logsID); err != nil {
		return err
	}
	if headsID == "" || logsID == "" || headsID == logsID {
		return ErrWSMalformed
	}
	if err := conn.SetReadDeadline(time.Now().Add(2 * c.Heartbeat)); err != nil {
		return ErrWSTransport
	}
	if onConnected != nil {
		onConnected()
	}
	beatCtx, stopBeat := context.WithCancel(ctx)
	beatDone := make(chan struct{})
	go func() {
		defer close(beatDone)
		ticker := time.NewTicker(c.Heartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-beatCtx.Done():
				return
			case <-ticker.C:
				if conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(c.DialTimeout)) != nil {
					_ = conn.Close()
					return
				}
			}
		}
	}()
	defer func() { stopBeat(); <-beatDone }()
	for {
		_, body, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return ErrWSTransport
		}
		hint, err := parseWSNotification(body, headsID, logsID)
		if err != nil {
			return err
		}
		onHint(hint)
	}
}

func (c *WSClient) call(conn *websocket.Conn, id int, method string, params any, out any) error {
	if err := conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}); err != nil {
		return ErrWSTransport
	}
	for skipped := 0; skipped < 1024; skipped++ {
		_, body, err := conn.ReadMessage()
		if err != nil {
			return ErrWSTransport
		}
		var response struct {
			JSONRPC string          `json:"jsonrpc"`
			ID      int             `json:"id"`
			Method  string          `json:"method"`
			Result  json.RawMessage `json:"result"`
			Error   *RPCError       `json:"error"`
		}
		if json.Unmarshal(body, &response) != nil || response.JSONRPC != "2.0" {
			return ErrWSMalformed
		}
		if response.Method == "eth_subscription" {
			// A head can arrive while the second subscription is being set up.
			// The mandatory HTTP sweep after setup recovers every skipped hint.
			continue
		}
		if response.ID != id {
			return ErrWSMalformed
		}
		if response.Error != nil {
			return response.Error
		}
		if len(response.Result) == 0 || bytes.Equal(response.Result, []byte("null")) || json.Unmarshal(response.Result, out) != nil {
			return ErrWSMalformed
		}
		return nil
	}
	return ErrWSMalformed
}

func parseWSNotification(body []byte, headsID, logsID string) (Hint, error) {
	var n struct {
		JSONRPC string `json:"jsonrpc"`
		Method  string `json:"method"`
		Params  struct {
			Subscription string          `json:"subscription"`
			Result       json.RawMessage `json:"result"`
		} `json:"params"`
		Error *RPCError `json:"error"`
	}
	if json.Unmarshal(body, &n) != nil || n.Error != nil || n.JSONRPC != "2.0" || n.Method != "eth_subscription" || len(n.Params.Result) == 0 {
		return Hint{}, ErrWSMalformed
	}
	switch n.Params.Subscription {
	case headsID:
		var raw rpcBlock
		if json.Unmarshal(n.Params.Result, &raw) != nil {
			return Hint{}, ErrWSMalformed
		}
		b, err := raw.parse()
		if err != nil || b.Hash == (common.Hash{}) || (b.Number > 0 && b.ParentHash == (common.Hash{})) {
			return Hint{}, ErrWSMalformed
		}
		return Hint{Kind: HeadHint, Number: b.Number, Hash: b.Hash}, nil
	case logsID:
		var raw rpcLog
		if json.Unmarshal(n.Params.Result, &raw) != nil {
			return Hint{}, ErrWSMalformed
		}
		removed := raw.Removed
		raw.Removed = false
		l, err := raw.parse()
		if err != nil || l.BlockHash == (common.Hash{}) {
			return Hint{}, ErrWSMalformed
		}
		return Hint{Kind: LogHint, Number: l.BlockNumber, Hash: l.BlockHash, Removed: removed}, nil
	default:
		return Hint{}, ErrWSMalformed
	}
}
