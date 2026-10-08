// Package store owns the PostgreSQL canonical append transaction.
package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"reorgguard/internal/crashprobe"
	"reorgguard/internal/model"
	"reorgguard/internal/telemetry"
	"reorgguard/migrations"
)

var ErrConfigMismatch = errors.New("stored dataset configuration mismatch")
var ErrCheckpoint = errors.New("checkpoint mismatch or gap")
var ErrReorgRequired = errors.New("canonical head differs; reorg reconciliation required")
var ErrUnsafe = errors.New("canonical progression halted pending operator review")

type Checkpoint struct {
	Number uint64
	Hash   common.Hash
}
type Dataset struct {
	ChainID    uint64
	Genesis    common.Hash
	FilterHash [32]byte
	StartBlock uint64
	Anchor     model.Block
}
type Store struct {
	db           *pgxpool.Pool
	ioTimeout    time.Duration
	beforeCommit func(context.Context, pgx.Tx) error // integration fault boundary
}

func Open(ctx context.Context, dsn string, timeout time.Duration) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("invalid database configuration")
	}
	return OpenConfig(ctx, cfg, timeout)
}
func OpenConfig(ctx context.Context, cfg *pgxpool.Config, timeout time.Duration) (*Store, error) {
	if timeout <= 0 {
		return nil, errors.New("database timeout must be positive")
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create database pool: %w", err)
	}
	s := &Store{db: pool, ioTimeout: timeout}
	work, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err = pool.Ping(work); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	if err = migrations.Apply(work, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate database: %w", err)
	}
	return s, nil
}
func (s *Store) Close() { s.db.Close() }

func (s *Store) Init(ctx context.Context, d Dataset) (Checkpoint, error) {
	if d.ChainID > math.MaxInt64 || d.StartBlock > math.MaxInt64 || d.Anchor.Hash == (common.Hash{}) || d.Genesis == (common.Hash{}) {
		return Checkpoint{}, ErrConfigMismatch
	}
	anchorNumber := uint64(0)
	if d.StartBlock > 0 {
		anchorNumber = d.StartBlock - 1
	}
	if d.Anchor.Number != anchorNumber || (d.StartBlock == 0 && d.Anchor.Hash != d.Genesis) {
		return Checkpoint{}, ErrConfigMismatch
	}
	work, cancel := context.WithTimeout(ctx, s.ioTimeout)
	defer cancel()
	work, txSpan := telemetry.Start(work, "db.dataset_init")
	defer txSpan.End()
	tx, err := s.db.Begin(work)
	if err != nil {
		return Checkpoint{}, fmt.Errorf("begin dataset init: %w", err)
	}
	defer rollback(tx, s.ioTimeout)
	if _, err = tx.Exec(work, `INSERT INTO blocks(chain_id,number,hash,parent_hash,block_time,canonical,logs_complete)
		VALUES($1,$2,$3,$4,$5,true,false) ON CONFLICT DO NOTHING`, int64(d.ChainID), int64(anchorNumber), d.Anchor.Hash[:], d.Anchor.ParentHash[:], d.Anchor.Time); err != nil {
		return Checkpoint{}, fmt.Errorf("insert anchor: %w", err)
	}
	var number int64
	var parent []byte
	var blockTime time.Time
	var canonical, complete bool
	if err = tx.QueryRow(work, `SELECT number,parent_hash,block_time,canonical,logs_complete FROM blocks WHERE chain_id=$1 AND hash=$2`, int64(d.ChainID), d.Anchor.Hash[:]).Scan(&number, &parent, &blockTime, &canonical, &complete); err != nil {
		return Checkpoint{}, fmt.Errorf("read anchor: %w", err)
	}
	if number != int64(anchorNumber) || !bytes.Equal(parent, d.Anchor.ParentHash[:]) || !blockTime.Equal(d.Anchor.Time) || !canonical || complete {
		return Checkpoint{}, model.ErrIdentity
	}
	if _, err = tx.Exec(work, `INSERT INTO sync_state(chain_id,genesis_hash,filter_hash,start_block,checkpoint_number,checkpoint_hash)
		VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, int64(d.ChainID), d.Genesis[:], d.FilterHash[:], int64(d.StartBlock), int64(anchorNumber), d.Anchor.Hash[:]); err != nil {
		return Checkpoint{}, fmt.Errorf("insert sync state: %w", err)
	}
	var genesis, filter, hash []byte
	var start, checkpoint int64
	var failReason *string
	if err = tx.QueryRow(work, `SELECT genesis_hash,filter_hash,start_block,checkpoint_number,checkpoint_hash,fail_reason FROM sync_state WHERE chain_id=$1 FOR UPDATE`, int64(d.ChainID)).Scan(&genesis, &filter, &start, &checkpoint, &hash, &failReason); err != nil {
		return Checkpoint{}, fmt.Errorf("read sync state: %w", err)
	}
	if !bytes.Equal(genesis, d.Genesis[:]) || !bytes.Equal(filter, d.FilterHash[:]) || start != int64(d.StartBlock) {
		return Checkpoint{}, ErrConfigMismatch
	}
	if failReason != nil {
		return Checkpoint{}, ErrUnsafe
	}
	var checkpointCanonical bool
	if err = tx.QueryRow(work, `SELECT EXISTS(SELECT 1 FROM blocks WHERE chain_id=$1 AND number=$2 AND hash=$3 AND canonical)`, int64(d.ChainID), checkpoint, hash).Scan(&checkpointCanonical); err != nil {
		return Checkpoint{}, fmt.Errorf("verify checkpoint: %w", err)
	}
	if !checkpointCanonical {
		return Checkpoint{}, ErrCorruptCanonical
	}
	if err = tx.Commit(work); err != nil {
		return Checkpoint{}, fmt.Errorf("commit dataset init: %w", err)
	}
	return Checkpoint{Number: uint64(checkpoint), Hash: common.BytesToHash(hash)}, nil
}

func (s *Store) Checkpoint(ctx context.Context, chainID uint64) (Checkpoint, error) {
	if chainID > math.MaxInt64 {
		return Checkpoint{}, ErrConfigMismatch
	}
	work, cancel := context.WithTimeout(ctx, s.ioTimeout)
	defer cancel()
	var n int64
	var hash []byte
	if err := s.db.QueryRow(work, `SELECT checkpoint_number,checkpoint_hash FROM sync_state WHERE chain_id=$1`, int64(chainID)).Scan(&n, &hash); err != nil {
		return Checkpoint{}, fmt.Errorf("read checkpoint: %w", err)
	}
	return Checkpoint{Number: uint64(n), Hash: common.BytesToHash(hash)}, nil
}

// Append accepts one contiguous completed batch. A full exact replay is a no-op.
// Partial overlap is rejected so the caller can re-read the durable frontier.
func (s *Store) Append(ctx context.Context, chainID uint64, items []model.BlockData) error {
	ctx, span := telemetry.Start(ctx, "db.canonical_append", attribute.Int("block.count", len(items)))
	defer span.End()
	if chainID > math.MaxInt64 || len(items) == 0 {
		return model.ErrInvalidRange
	}
	blocks := make([]model.Block, len(items))
	var logs []model.Log
	for i, item := range items {
		blocks[i] = item.Block
		logs = append(logs, item.Logs...)
	}
	if blocks[0].Number > math.MaxInt64 || blocks[len(blocks)-1].Number > math.MaxInt64 {
		return model.ErrInvalidRange
	}
	// This also checks each supplied log against its block and rejects changed
	// content hidden behind an inconsistent claimed fingerprint.
	validated, err := model.BuildRange(blocks[0].Number, blocks[len(blocks)-1].Number, blocks[0].ParentHash, blocks, logs)
	if err != nil {
		return err
	}
	for i := range validated {
		if validated[i].LogSetHash != items[i].LogSetHash {
			return model.ErrIdentity
		}
	}
	work, cancel := context.WithTimeout(ctx, s.ioTimeout)
	defer cancel()
	work, txSpan := telemetry.Start(work, "db.append_transaction")
	defer txSpan.End()
	tx, err := s.db.Begin(work)
	if err != nil {
		return fmt.Errorf("begin canonical append: %w", err)
	}
	defer rollback(tx, s.ioTimeout)
	var oldNumber int64
	var oldHash []byte
	var failReason *string
	if err = tx.QueryRow(work, `SELECT checkpoint_number,checkpoint_hash,fail_reason FROM sync_state WHERE chain_id=$1 FOR UPDATE`, int64(chainID)).Scan(&oldNumber, &oldHash, &failReason); err != nil {
		return fmt.Errorf("lock checkpoint: %w", err)
	}
	if failReason != nil {
		return ErrUnsafe
	}
	var checkpointCanonical bool
	if err = tx.QueryRow(work, `SELECT EXISTS(SELECT 1 FROM blocks WHERE chain_id=$1 AND number=$2 AND hash=$3 AND canonical)`, int64(chainID), oldNumber, oldHash).Scan(&checkpointCanonical); err != nil {
		return fmt.Errorf("verify checkpoint: %w", err)
	}
	if !checkpointCanonical {
		return ErrCorruptCanonical
	}
	first, last := blocks[0].Number, blocks[len(blocks)-1].Number
	if last <= uint64(oldNumber) {
		for _, item := range validated {
			if err = verifyBlock(work, tx, chainID, item); err != nil {
				return err
			}
		}
		return tx.Commit(work)
	}
	if first != uint64(oldNumber)+1 {
		return ErrCheckpoint
	}
	if !bytes.Equal(blocks[0].ParentHash[:], oldHash) {
		return ErrReorgRequired
	}
	for i, item := range validated {
		if _, err = persistBlock(work, tx, chainID, item); err != nil {
			return err
		}
		if i == 0 {
			if err = crashprobe.Pause(work, "append_after_first_block"); err != nil {
				return err
			}
		}
	}
	if err = crashprobe.Pause(work, "append_before_checkpoint"); err != nil {
		return err
	}
	checkpointCtx, checkpointSpan := telemetry.Start(work, "db.checkpoint_update")
	if _, err = tx.Exec(checkpointCtx, `UPDATE sync_state SET checkpoint_number=$2,checkpoint_hash=$3,checkpoint_updated_at=now(),updated_at=now()
		WHERE chain_id=$1`, int64(chainID), int64(last), blocks[len(blocks)-1].Hash[:]); err != nil {
		telemetry.Failure(checkpointSpan, "database_failure")
		checkpointSpan.End()
		return fmt.Errorf("advance checkpoint: %w", err)
	}
	checkpointSpan.End()
	if s.beforeCommit != nil {
		if err = s.beforeCommit(work, tx); err != nil {
			return fmt.Errorf("before commit: %w", err)
		}
	}
	if err = crashprobe.Pause(work, "append_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(work); err != nil {
		return fmt.Errorf("commit canonical batch: %w", err)
	}
	if err = crashprobe.Pause(ctx, "append_after_commit"); err != nil {
		return err
	}
	return nil
}

func persistBlock(ctx context.Context, tx pgx.Tx, chainID uint64, item model.BlockData) (bool, error) {
	b := item.Block
	tag, err := tx.Exec(ctx, `INSERT INTO blocks(chain_id,number,hash,parent_hash,block_time,canonical,logs_complete,log_count,log_set_hash)
		VALUES($1,$2,$3,$4,$5,false,true,$6,$7) ON CONFLICT DO NOTHING`, int64(chainID), int64(b.Number), b.Hash[:], b.ParentHash[:], b.Time, int64(len(item.Logs)), item.LogSetHash[:])
	if err != nil {
		return false, fmt.Errorf("insert block %d: %w", b.Number, err)
	}
	seen := tag.RowsAffected() == 0
	if err := verifyBlockFields(ctx, tx, chainID, item, false); err != nil {
		return false, err
	}
	for _, l := range item.Logs {
		topics := make([][]byte, len(l.Topics))
		for i, t := range l.Topics {
			topics[i] = t[:]
		}
		if _, err := tx.Exec(ctx, `INSERT INTO logs(chain_id,block_hash,block_number,tx_hash,tx_index,log_index,address,topics,data)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT DO NOTHING`, int64(chainID), l.BlockHash[:], int64(l.BlockNumber), l.TxHash[:], int32(l.TxIndex), int32(l.Index), l.Address[:], topics, l.Data); err != nil {
			return false, fmt.Errorf("insert log at block %d index %d: %w", l.BlockNumber, l.Index, err)
		}
		if err := verifyLog(ctx, tx, chainID, l); err != nil {
			return false, err
		}
	}
	var actual int64
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM logs WHERE chain_id=$1 AND block_hash=$2`, int64(chainID), b.Hash[:]).Scan(&actual); err != nil {
		return false, fmt.Errorf("count stored logs: %w", err)
	}
	if actual != int64(len(item.Logs)) {
		return false, model.ErrIdentity
	}
	if _, err = tx.Exec(ctx, `UPDATE blocks SET canonical=true WHERE chain_id=$1 AND hash=$2`, int64(chainID), b.Hash[:]); err != nil {
		return false, fmt.Errorf("activate block %d: %w", b.Number, err)
	}
	return seen, nil
}
func verifyBlock(ctx context.Context, tx pgx.Tx, chainID uint64, item model.BlockData) error {
	if err := verifyBlockFields(ctx, tx, chainID, item, true); err != nil {
		return err
	}
	var count int64
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM logs WHERE chain_id=$1 AND block_hash=$2`, int64(chainID), item.Block.Hash[:]).Scan(&count); err != nil {
		return fmt.Errorf("count stored logs: %w", err)
	}
	if count != int64(len(item.Logs)) {
		return model.ErrIdentity
	}
	for _, l := range item.Logs {
		if err := verifyLog(ctx, tx, chainID, l); err != nil {
			return err
		}
	}
	return nil
}
func verifyBlockFields(ctx context.Context, tx pgx.Tx, chainID uint64, item model.BlockData, requireCanonical bool) error {
	var number, count int64
	var parent, digest []byte
	var blockTime time.Time
	var canonical, complete bool
	if err := tx.QueryRow(ctx, `SELECT number,parent_hash,block_time,canonical,logs_complete,log_count,log_set_hash
		FROM blocks WHERE chain_id=$1 AND hash=$2`, int64(chainID), item.Block.Hash[:]).Scan(&number, &parent, &blockTime, &canonical, &complete, &count, &digest); err != nil {
		return fmt.Errorf("read stored block: %w", err)
	}
	if number != int64(item.Block.Number) || !bytes.Equal(parent, item.Block.ParentHash[:]) || !blockTime.Equal(item.Block.Time) || (requireCanonical && !canonical) || !complete || count != int64(len(item.Logs)) || !bytes.Equal(digest, item.LogSetHash[:]) {
		return model.ErrIdentity
	}
	return nil
}
func verifyLog(ctx context.Context, tx pgx.Tx, chainID uint64, l model.Log) error {
	var number int64
	var txHash, address, data []byte
	var txIndex, logIndex int32
	var topics [][]byte
	if err := tx.QueryRow(ctx, `SELECT block_number,tx_hash,tx_index,log_index,address,topics,data FROM logs
		WHERE chain_id=$1 AND block_hash=$2 AND log_index=$3`, int64(chainID), l.BlockHash[:], int32(l.Index)).Scan(&number, &txHash, &txIndex, &logIndex, &address, &topics, &data); err != nil {
		return fmt.Errorf("read stored log: %w", err)
	}
	if number != int64(l.BlockNumber) || !bytes.Equal(txHash, l.TxHash[:]) || txIndex != int32(l.TxIndex) || logIndex != int32(l.Index) || !bytes.Equal(address, l.Address[:]) || !bytes.Equal(data, l.Data) || len(topics) != len(l.Topics) {
		return model.ErrIdentity
	}
	for i, t := range l.Topics {
		if !bytes.Equal(topics[i], t[:]) {
			return model.ErrIdentity
		}
	}
	return nil
}

func rollback(tx pgx.Tx, timeout time.Duration) {
	if timeout > 2*time.Second {
		timeout = 2 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_ = tx.Rollback(ctx)
}
