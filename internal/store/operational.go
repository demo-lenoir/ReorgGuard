package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"reorgguard/internal/model"
)

var ErrRevisionChanged = errors.New("canonical revision changed")
var ErrPageTooLarge = errors.New("log payload exceeds operational API limit")

const MaxAPIDataBytes = 32 << 10

type Snapshot struct {
	Checkpoint Checkpoint
	Revision   uint64
	UpdatedAt  time.Time
	LastReorg  json.RawMessage
	Unsafe     string
}

func (s *Store) Snapshot(ctx context.Context, chainID uint64) (Snapshot, error) {
	if chainID > math.MaxInt64 {
		return Snapshot{}, ErrConfigMismatch
	}
	work, cancel := context.WithTimeout(ctx, s.ioTimeout)
	defer cancel()
	var n, revision int64
	var hash []byte
	var summary []byte
	var reason *string
	var at time.Time
	err := s.db.QueryRow(work, `SELECT checkpoint_number,checkpoint_hash,canonical_revision,checkpoint_updated_at,last_reorg,fail_reason FROM sync_state WHERE chain_id=$1`, int64(chainID)).Scan(&n, &hash, &revision, &at, &summary, &reason)
	if err != nil {
		return Snapshot{}, fmt.Errorf("read operational snapshot: %w", err)
	}
	out := Snapshot{Checkpoint: Checkpoint{Number: uint64(n), Hash: common.BytesToHash(hash)}, Revision: uint64(revision), UpdatedAt: at, LastReorg: summary}
	if reason != nil {
		out.Unsafe = *reason
	}
	return out, nil
}

type LogKey struct {
	BlockNumber uint64
	LogIndex    uint32
	BlockHash   common.Hash
}
type LogQuery struct {
	From, To uint64
	Address  *common.Address
	Topic0   *common.Hash
	Limit    int
	After    *LogKey
	Revision *uint64
}
type LogPage struct {
	Items    []model.Log
	Next     *LogKey
	Revision uint64
}

// ListLogs uses one read-only repeatable-read snapshot so the revision and
// canonical rows always describe the same database state. Reorgs invalidate
// cross-request cursors; append-only progress leaves them valid.
func (s *Store) ListLogs(ctx context.Context, chainID uint64, q LogQuery) (LogPage, error) {
	if chainID > math.MaxInt64 || q.From > q.To || q.To > math.MaxInt64 || q.Limit < 1 || q.Limit > 100 {
		return LogPage{}, model.ErrInvalidRange
	}
	work, cancel := context.WithTimeout(ctx, s.ioTimeout)
	defer cancel()
	tx, err := s.db.BeginTx(work, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return LogPage{}, fmt.Errorf("begin log page: %w", err)
	}
	defer rollback(tx, s.ioTimeout)
	var revision int64
	if err = tx.QueryRow(work, `SELECT canonical_revision FROM sync_state WHERE chain_id=$1`, int64(chainID)).Scan(&revision); err != nil {
		return LogPage{}, fmt.Errorf("read page revision: %w", err)
	}
	if q.Revision != nil && *q.Revision != uint64(revision) {
		return LogPage{}, ErrRevisionChanged
	}
	var address, topic, afterHash []byte
	if q.Address != nil {
		address = q.Address[:]
	}
	if q.Topic0 != nil {
		topic = q.Topic0[:]
	}
	var afterNumber int64 = -1
	var afterIndex int32 = -1
	if q.After != nil {
		if q.After.BlockNumber > math.MaxInt64 || q.After.LogIndex > math.MaxInt32 {
			return LogPage{}, model.ErrInvalidRange
		}
		afterNumber, afterIndex, afterHash = int64(q.After.BlockNumber), int32(q.After.LogIndex), q.After.BlockHash[:]
	}
	rows, err := tx.Query(work, `SELECT l.block_hash,l.block_number,l.tx_hash,l.tx_index,l.log_index,l.address,l.topics,
		CASE WHEN octet_length(l.data)<=$10 THEN l.data ELSE NULL END,octet_length(l.data)
		FROM logs l JOIN blocks b ON b.chain_id=l.chain_id AND b.hash=l.block_hash
		WHERE l.chain_id=$1 AND b.canonical AND l.block_number BETWEEN $2 AND $3
		AND ($4::bytea IS NULL OR l.address=$4)
		AND ($5::bytea IS NULL OR l.topics[1]=$5)
		AND (l.block_number,l.log_index,l.block_hash) > ($6,$7,$8::bytea)
		ORDER BY l.block_number,l.log_index,l.block_hash LIMIT $9`, int64(chainID), int64(q.From), int64(q.To), address, topic, afterNumber, afterIndex, afterHash, q.Limit+1, MaxAPIDataBytes)
	if err != nil {
		return LogPage{}, fmt.Errorf("query log page: %w", err)
	}
	out := LogPage{Items: make([]model.Log, 0, q.Limit), Revision: uint64(revision)}
	for rows.Next() {
		var hash, txHash, addr, data []byte
		var number int64
		var txIndex, logIndex int32
		var dataBytes int32
		var topics [][]byte
		if err = rows.Scan(&hash, &number, &txHash, &txIndex, &logIndex, &addr, &topics, &data, &dataBytes); err != nil {
			rows.Close()
			return LogPage{}, fmt.Errorf("scan log page: %w", err)
		}
		if dataBytes > MaxAPIDataBytes {
			if len(out.Items) >= q.Limit {
				last := out.Items[len(out.Items)-1]
				out.Next = &LogKey{BlockNumber: last.BlockNumber, LogIndex: last.Index, BlockHash: last.BlockHash}
				break
			}
			rows.Close()
			return LogPage{}, ErrPageTooLarge
		}
		l := model.Log{BlockHash: common.BytesToHash(hash), BlockNumber: uint64(number), TxHash: common.BytesToHash(txHash), TxIndex: uint32(txIndex), Index: uint32(logIndex), Address: common.BytesToAddress(addr), Data: data}
		for _, t := range topics {
			l.Topics = append(l.Topics, common.BytesToHash(t))
		}
		out.Items = append(out.Items, l)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return LogPage{}, fmt.Errorf("iterate log page: %w", err)
	}
	rows.Close()
	if len(out.Items) > q.Limit {
		out.Items = out.Items[:q.Limit]
		last := out.Items[len(out.Items)-1]
		out.Next = &LogKey{BlockNumber: last.BlockNumber, LogIndex: last.Index, BlockHash: last.BlockHash}
	}
	if err = tx.Commit(work); err != nil {
		return LogPage{}, fmt.Errorf("commit log page read: %w", err)
	}
	return out, nil
}
