package backfill

import (
	"context"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"reorgguard/internal/model"
	"reorgguard/internal/rpc"
	"reorgguard/internal/store"
)

var ErrCheckpointHistory = errors.New("checkpoint history unavailable")
var ErrCheckpointMoved = errors.New("checkpoint height changed during verification")

// CheckpointSource is the complete boundary required to verify a durable
// checkpoint, including the old block when the provider reports a new fork.
type CheckpointSource interface {
	BlockByNumber(context.Context, uint64) (model.Block, error)
	BlockByHash(context.Context, common.Hash) (model.Block, error)
	Logs(context.Context, uint64, uint64, rpc.Filter) ([]model.Log, error)
	LogsByBlockHash(context.Context, common.Hash, rpc.Filter) ([]model.Log, error)
}

type CheckpointRepository interface {
	CanonicalBlockByNumber(context.Context, uint64, uint64) (model.Block, error)
	CanonicalLogs(context.Context, uint64, uint64, uint64) ([]model.Log, error)
}

// VerifyCheckpoint is used by both direct backfill and pooled eligibility.
// A fork result grants no canonical authority: the caller must prove ancestry
// and switch through the ordinary reorg transaction before advancing.
func VerifyCheckpoint(ctx context.Context, source CheckpointSource, repo CheckpointRepository, chainID uint64, cp store.Checkpoint, startBlock uint64, filter rpc.Filter) (fork bool, err error) {
	local, err := repo.CanonicalBlockByNumber(ctx, chainID, cp.Number)
	if err != nil {
		return false, err
	}
	if local.Number != cp.Number || local.Hash != cp.Hash {
		return false, store.ErrCorruptCanonical
	}
	byNumber, err := source.BlockByNumber(ctx, cp.Number)
	if err != nil {
		return false, err
	}
	if byNumber.Number != cp.Number || byNumber.Hash == (common.Hash{}) {
		return false, fmt.Errorf("checkpoint height: %w", model.ErrIdentity)
	}
	byHash, err := source.BlockByHash(ctx, cp.Hash)
	if err != nil {
		if errors.Is(err, rpc.ErrNotFound) {
			return false, fmt.Errorf("old checkpoint block: %w", ErrCheckpointHistory)
		}
		return false, err
	}
	if !sameBlockIdentity(local, byHash) || (byNumber.Hash == cp.Hash && !sameBlockIdentity(local, byNumber)) {
		return false, fmt.Errorf("checkpoint header: %w", model.ErrIdentity)
	}
	// The starting anchor was not indexed and has no completed filtered log
	// set. Header identity is checked by hash; height observations can move.
	checkSet := cp.Number > 0 && cp.Number >= startBlock
	if !checkSet {
		return byNumber.Hash != cp.Hash, nil
	}
	hashLogs, err := source.LogsByBlockHash(ctx, cp.Hash, filter)
	if err != nil {
		return false, err
	}
	if err = filter.ValidateLogs(hashLogs); err != nil {
		return false, err
	}
	var stored []model.Log
	if checkSet {
		stored, err = repo.CanonicalLogs(ctx, chainID, cp.Number, cp.Number)
		if err != nil {
			return false, err
		}
		if !sameCheckpointLogs(local, stored, hashLogs) {
			return false, fmt.Errorf("checkpoint filtered log set: %w", model.ErrIdentity)
		}
	}
	rangeLogs, err := source.Logs(ctx, cp.Number, cp.Number, filter)
	if err != nil {
		return false, err
	}
	if err = validateCheckpointRangeLogs(cp.Hash, stored, rangeLogs); err != nil {
		return false, err
	}
	if err = filter.ValidateLogs(rangeLogs); err != nil {
		return false, err
	}
	// A range query is bound to height, not the old checkpoint hash. Recheck
	// which block occupies that height before interpreting its log set. If the
	// height moved, the ordinary bounded reorg path must establish the branch.
	latest, err := source.BlockByNumber(ctx, cp.Number)
	if err != nil {
		return false, err
	}
	if latest.Number != cp.Number || latest.Hash == (common.Hash{}) {
		return false, fmt.Errorf("latest checkpoint height: %w", model.ErrIdentity)
	}
	if latest.Hash == cp.Hash && !sameBlockIdentity(local, latest) {
		return false, fmt.Errorf("latest checkpoint header: %w", model.ErrIdentity)
	}
	if latest.Hash != cp.Hash && byNumber.Hash == cp.Hash {
		return true, nil
	}
	if latest.Hash != byNumber.Hash || (latest.Hash == cp.Hash && !sameCheckpointLogs(local, stored, rangeLogs)) {
		return false, ErrCheckpointMoved
	}
	return latest.Hash != cp.Hash, nil
}

// A height query can observe a different branch, but each returned log still
// names its immutable block hash. Any row naming the completed checkpoint must
// reproduce a known row exactly, even if a later header lookup observes a fork.
// Absence of a row is not hash-bound evidence: an empty/new-branch range result
// remains subject to the bounded movement/reorg classification above.
func validateCheckpointRangeLogs(hash common.Hash, stored, observed []model.Log) error {
	known := make(map[uint32]model.Log, len(stored))
	for _, log := range stored {
		known[log.Index] = log
	}
	for _, log := range observed {
		if log.BlockHash != hash {
			continue
		}
		expected, ok := known[log.Index]
		if !ok || !model.EqualLog(expected, log) {
			return fmt.Errorf("checkpoint range log identity: %w", model.ErrIdentity)
		}
	}
	return nil
}

func sameBlockIdentity(a, b model.Block) bool {
	return a.Number == b.Number && a.Hash == b.Hash && a.ParentHash == b.ParentHash && a.Time.Equal(b.Time)
}

func sameCheckpointLogs(block model.Block, stored, observed []model.Log) bool {
	items, err := model.BuildRange(block.Number, block.Number, block.ParentHash, []model.Block{block}, observed)
	return err == nil && len(items) == 1 && items[0].LogSetHash == model.FingerprintLogs(stored)
}
