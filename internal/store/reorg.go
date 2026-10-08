package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"reorgguard/internal/crashprobe"
	"reorgguard/internal/model"
	"reorgguard/internal/telemetry"
)

var ErrBlockNotFound = errors.New("canonical block not found")
var ErrCorruptCanonical = errors.New("stored canonical parent chain is corrupt")
var ErrDepth = errors.New("reorg exceeds configured maximum depth")

type StoredBlock struct {
	Block        model.Block
	Canonical    bool
	LogsComplete bool
}
type ReorgResult struct {
	Ancestor              Checkpoint
	OldHead               Checkpoint
	NewHead               Checkpoint
	Depth                 uint64
	OrphanedBlocks        uint64
	OrphanedLogs          uint64
	RecanonicalizedBlocks uint64
	NewBlocks             uint64
}
type Readiness struct {
	State      string
	Reason     string
	Checkpoint Checkpoint
}

func (s *Store) CanonicalHead(ctx context.Context, chainID uint64) (model.Block, error) {
	if chainID > math.MaxInt64 {
		return model.Block{}, ErrConfigMismatch
	}
	work, cancel := context.WithTimeout(ctx, s.ioTimeout)
	defer cancel()
	var n int64
	var hash, parent []byte
	var ts time.Time
	err := s.db.QueryRow(work, `SELECT b.number,b.hash,b.parent_hash,b.block_time FROM sync_state st
		JOIN blocks b ON b.chain_id=st.chain_id AND b.hash=st.checkpoint_hash AND b.number=st.checkpoint_number
		WHERE st.chain_id=$1 AND b.canonical`, int64(chainID)).Scan(&n, &hash, &parent, &ts)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Block{}, ErrBlockNotFound
	}
	if err != nil {
		return model.Block{}, fmt.Errorf("read canonical head: %w", err)
	}
	return model.Block{Number: uint64(n), Hash: common.BytesToHash(hash), ParentHash: common.BytesToHash(parent), Time: ts}, nil
}
func (s *Store) CanonicalBlockByNumber(ctx context.Context, chainID, n uint64) (model.Block, error) {
	if chainID > math.MaxInt64 || n > math.MaxInt64 {
		return model.Block{}, ErrConfigMismatch
	}
	work, cancel := context.WithTimeout(ctx, s.ioTimeout)
	defer cancel()
	var hash, parent []byte
	var ts time.Time
	err := s.db.QueryRow(work, `SELECT hash,parent_hash,block_time FROM blocks WHERE chain_id=$1 AND number=$2 AND canonical`, int64(chainID), int64(n)).Scan(&hash, &parent, &ts)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Block{}, ErrBlockNotFound
	}
	if err != nil {
		return model.Block{}, fmt.Errorf("read canonical block %d: %w", n, err)
	}
	return model.Block{Number: n, Hash: common.BytesToHash(hash), ParentHash: common.BytesToHash(parent), Time: ts}, nil
}
func (s *Store) BlockByHash(ctx context.Context, chainID uint64, h common.Hash) (StoredBlock, error) {
	if chainID > math.MaxInt64 {
		return StoredBlock{}, ErrConfigMismatch
	}
	work, cancel := context.WithTimeout(ctx, s.ioTimeout)
	defer cancel()
	var n int64
	var parent []byte
	var ts time.Time
	var canonical, complete bool
	err := s.db.QueryRow(work, `SELECT number,parent_hash,block_time,canonical,logs_complete FROM blocks WHERE chain_id=$1 AND hash=$2`, int64(chainID), h[:]).Scan(&n, &parent, &ts, &canonical, &complete)
	if errors.Is(err, pgx.ErrNoRows) {
		return StoredBlock{}, ErrBlockNotFound
	}
	if err != nil {
		return StoredBlock{}, fmt.Errorf("read block by hash: %w", err)
	}
	return StoredBlock{Block: model.Block{Number: uint64(n), Hash: h, ParentHash: common.BytesToHash(parent), Time: ts}, Canonical: canonical, LogsComplete: complete}, nil
}

// Reconcile switches one proven local suffix to a fully fetched replacement
// branch. It never performs RPC while holding the transaction.
func (s *Store) Reconcile(ctx context.Context, chainID uint64, oldHead, ancestor Checkpoint, replacement []model.BlockData, maxDepth uint64) (ReorgResult, error) {
	ctx, span := telemetry.Start(ctx, "db.reconcile", attribute.Int("replacement.blocks", len(replacement)))
	defer span.End()
	if chainID > math.MaxInt64 || len(replacement) == 0 || maxDepth == 0 || oldHead.Number > math.MaxInt64 || ancestor.Number >= oldHead.Number || oldHead.Number-ancestor.Number > maxDepth {
		return ReorgResult{}, ErrDepth
	}
	blocks := make([]model.Block, len(replacement))
	var logs []model.Log
	for i, item := range replacement {
		blocks[i] = item.Block
		logs = append(logs, item.Logs...)
	}
	last := blocks[len(blocks)-1].Number
	if blocks[0].Number != ancestor.Number+1 || last > math.MaxInt64 {
		return ReorgResult{}, model.ErrInvalidRange
	}
	validated, err := model.BuildRange(blocks[0].Number, last, ancestor.Hash, blocks, logs)
	if err != nil {
		return ReorgResult{}, err
	}
	for i := range validated {
		if validated[i].LogSetHash != replacement[i].LogSetHash {
			return ReorgResult{}, model.ErrIdentity
		}
	}
	work, cancel := context.WithTimeout(ctx, s.ioTimeout)
	defer cancel()
	work, txSpan := telemetry.Start(work, "db.reconciliation_transaction")
	defer txSpan.End()
	tx, err := s.db.Begin(work)
	if err != nil {
		return ReorgResult{}, fmt.Errorf("begin reconciliation: %w", err)
	}
	defer rollback(tx, s.ioTimeout)
	var cpNumber int64
	var cpHash []byte
	var failReason *string
	if err = tx.QueryRow(work, `SELECT checkpoint_number,checkpoint_hash,fail_reason FROM sync_state WHERE chain_id=$1 FOR UPDATE`, int64(chainID)).Scan(&cpNumber, &cpHash, &failReason); err != nil {
		return ReorgResult{}, fmt.Errorf("lock checkpoint: %w", err)
	}
	if failReason != nil {
		return ReorgResult{}, ErrUnsafe
	}
	if cpNumber != int64(oldHead.Number) || !bytes.Equal(cpHash, oldHead.Hash[:]) {
		return ReorgResult{}, ErrCheckpoint
	}
	var anchorHash []byte
	if err = tx.QueryRow(work, `SELECT hash FROM blocks WHERE chain_id=$1 AND number=$2 AND canonical FOR UPDATE`, int64(chainID), int64(ancestor.Number)).Scan(&anchorHash); errors.Is(err, pgx.ErrNoRows) {
		return ReorgResult{}, ErrBlockNotFound
	} else if err != nil {
		return ReorgResult{}, fmt.Errorf("lock ancestor: %w", err)
	}
	if !bytes.Equal(anchorHash, ancestor.Hash[:]) {
		return ReorgResult{}, ErrCorruptCanonical
	}
	// Prove the stored old suffix too; a plausible height sequence is insufficient.
	expected := oldHead.Hash
	for n := oldHead.Number; n > ancestor.Number; n-- {
		var hash, parent []byte
		if err = tx.QueryRow(work, `SELECT hash,parent_hash FROM blocks WHERE chain_id=$1 AND number=$2 AND canonical FOR UPDATE`, int64(chainID), int64(n)).Scan(&hash, &parent); errors.Is(err, pgx.ErrNoRows) {
			return ReorgResult{}, ErrBlockNotFound
		} else if err != nil {
			return ReorgResult{}, fmt.Errorf("read old suffix %d: %w", n, err)
		}
		if !bytes.Equal(hash, expected[:]) {
			return ReorgResult{}, ErrCorruptCanonical
		}
		expected = common.BytesToHash(parent)
	}
	if expected != ancestor.Hash {
		return ReorgResult{}, ErrCorruptCanonical
	}
	result := ReorgResult{Ancestor: ancestor, OldHead: oldHead, NewHead: Checkpoint{Number: last, Hash: blocks[len(blocks)-1].Hash}, Depth: oldHead.Number - ancestor.Number, OrphanedBlocks: oldHead.Number - ancestor.Number}
	var orphanedLogs int64
	if err = tx.QueryRow(work, `SELECT count(*) FROM logs l JOIN blocks b ON b.chain_id=l.chain_id AND b.hash=l.block_hash
		WHERE b.chain_id=$1 AND b.canonical AND b.number>$2 AND b.number<=$3`, int64(chainID), int64(ancestor.Number), int64(oldHead.Number)).Scan(&orphanedLogs); err != nil {
		return ReorgResult{}, fmt.Errorf("count orphaned logs: %w", err)
	}
	result.OrphanedLogs = uint64(orphanedLogs)
	tag, err := tx.Exec(work, `UPDATE blocks SET canonical=false WHERE chain_id=$1 AND number>$2 AND number<=$3 AND canonical`, int64(chainID), int64(ancestor.Number), int64(oldHead.Number))
	if err != nil {
		return ReorgResult{}, fmt.Errorf("orphan old suffix: %w", err)
	}
	if uint64(tag.RowsAffected()) != result.Depth {
		return ReorgResult{}, ErrCorruptCanonical
	}
	if err = crashprobe.Pause(work, "reorg_after_orphan"); err != nil {
		return ReorgResult{}, err
	}
	for i, item := range validated {
		seen, persistErr := persistBlock(work, tx, chainID, item)
		if persistErr != nil {
			return ReorgResult{}, persistErr
		}
		if seen {
			result.RecanonicalizedBlocks++
		} else {
			result.NewBlocks++
		}
		if i == 0 {
			if err = crashprobe.Pause(work, "reorg_after_first_activation"); err != nil {
				return ReorgResult{}, err
			}
		}
	}
	summary, err := json.Marshal(map[string]any{
		"ancestor_height": ancestor.Number, "ancestor_hash": ancestor.Hash.Hex(),
		"old_head_height": oldHead.Number, "old_head_hash": oldHead.Hash.Hex(),
		"new_head_height": result.NewHead.Number, "new_head_hash": result.NewHead.Hash.Hex(),
		"depth": result.Depth, "orphaned_blocks": result.OrphanedBlocks,
		"orphaned_logs": result.OrphanedLogs,
	})
	if err != nil {
		return ReorgResult{}, err
	}
	checkpointCtx, checkpointSpan := telemetry.Start(work, "db.checkpoint_update")
	if _, err = tx.Exec(checkpointCtx, `UPDATE sync_state SET checkpoint_number=$2,checkpoint_hash=$3,canonical_revision=canonical_revision+1,last_reorg=$4,checkpoint_updated_at=now(),updated_at=now()
		WHERE chain_id=$1`, int64(chainID), int64(last), result.NewHead.Hash[:], summary); err != nil {
		telemetry.Failure(checkpointSpan, "database_failure")
		checkpointSpan.End()
		return ReorgResult{}, fmt.Errorf("replace checkpoint: %w", err)
	}
	checkpointSpan.End()
	if s.beforeCommit != nil {
		if err = s.beforeCommit(work, tx); err != nil {
			return ReorgResult{}, fmt.Errorf("before reconciliation commit: %w", err)
		}
	}
	if err = crashprobe.Pause(work, "reorg_before_commit"); err != nil {
		return ReorgResult{}, err
	}
	if err = tx.Commit(work); err != nil {
		return ReorgResult{}, fmt.Errorf("commit reconciliation: %w", err)
	}
	if err = crashprobe.Pause(ctx, "reorg_after_commit"); err != nil {
		return ReorgResult{}, err
	}
	return result, nil
}

func (s *Store) MarkUnsafe(ctx context.Context, chainID uint64, reason string) error {
	if chainID > math.MaxInt64 || !validFailureReason(reason) {
		return ErrConfigMismatch
	}
	work, cancel := context.WithTimeout(ctx, s.ioTimeout)
	defer cancel()
	tag, err := s.db.Exec(work, `UPDATE sync_state SET fail_reason=$2,updated_at=now() WHERE chain_id=$1`, int64(chainID), reason)
	if err != nil {
		return fmt.Errorf("mark unsafe: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrCheckpoint
	}
	return nil
}
func validFailureReason(reason string) bool {
	switch reason {
	case "depth_exceeded", "ancestor_missing", "parent_chain_invalid", "identity_conflict", "provider_inconsistent":
		return true
	}
	return false
}
func (s *Store) Readiness(ctx context.Context, chainID uint64) (Readiness, error) {
	if chainID > math.MaxInt64 {
		return Readiness{}, ErrConfigMismatch
	}
	work, cancel := context.WithTimeout(ctx, s.ioTimeout)
	defer cancel()
	var n int64
	var hash []byte
	var reason *string
	var checkpointCanonical bool
	err := s.db.QueryRow(work, `SELECT st.checkpoint_number,st.checkpoint_hash,st.fail_reason,
		EXISTS(SELECT 1 FROM blocks b WHERE b.chain_id=st.chain_id AND b.number=st.checkpoint_number AND b.hash=st.checkpoint_hash AND b.canonical)
		FROM sync_state st WHERE st.chain_id=$1`, int64(chainID)).Scan(&n, &hash, &reason, &checkpointCanonical)
	if err != nil {
		return Readiness{}, fmt.Errorf("read readiness: %w", err)
	}
	state := Readiness{State: "healthy", Checkpoint: Checkpoint{Number: uint64(n), Hash: common.BytesToHash(hash)}}
	if reason != nil {
		state.State = "unsafe"
		state.Reason = *reason
		return state, nil
	}
	if !checkpointCanonical {
		state.State = "unsafe"
		state.Reason = "parent_chain_invalid"
	}
	return state, nil
}

// CanonicalLogs returns the currently visible filtered logs in deterministic order.
func (s *Store) CanonicalLogs(ctx context.Context, chainID, from, to uint64) ([]model.Log, error) {
	if chainID > math.MaxInt64 || from > to || to > math.MaxInt64 {
		return nil, model.ErrInvalidRange
	}
	work, cancel := context.WithTimeout(ctx, s.ioTimeout)
	defer cancel()
	rows, err := s.db.Query(work, `SELECT l.block_hash,l.block_number,l.tx_hash,l.tx_index,l.log_index,l.address,l.topics,l.data
		FROM logs l JOIN blocks b ON b.chain_id=l.chain_id AND b.hash=l.block_hash
		WHERE l.chain_id=$1 AND b.canonical AND b.number BETWEEN $2 AND $3 ORDER BY b.number,l.log_index`, int64(chainID), int64(from), int64(to))
	if err != nil {
		return nil, fmt.Errorf("query canonical logs: %w", err)
	}
	defer rows.Close()
	var out []model.Log
	for rows.Next() {
		var hash, txHash, address, data []byte
		var number int64
		var txIndex, logIndex int32
		var topics [][]byte
		if err = rows.Scan(&hash, &number, &txHash, &txIndex, &logIndex, &address, &topics, &data); err != nil {
			return nil, fmt.Errorf("scan canonical log: %w", err)
		}
		l := model.Log{BlockHash: common.BytesToHash(hash), BlockNumber: uint64(number), TxHash: common.BytesToHash(txHash), TxIndex: uint32(txIndex), Index: uint32(logIndex), Address: common.BytesToAddress(address), Data: data}
		for _, t := range topics {
			l.Topics = append(l.Topics, common.BytesToHash(t))
		}
		out = append(out, l)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate canonical logs: %w", err)
	}
	return out, nil
}
