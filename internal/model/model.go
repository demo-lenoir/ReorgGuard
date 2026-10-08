// Package model contains the canonical append rules without RPC or SQL dependencies.
package model

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

var ErrInvalidRange = errors.New("invalid canonical range")
var ErrIdentity = errors.New("conflicting immutable identity")
var ErrParent = errors.New("parent hash mismatch")

type Block struct {
	Number     uint64
	Hash       common.Hash
	ParentHash common.Hash
	Time       time.Time
}

type Log struct {
	BlockHash   common.Hash
	BlockNumber uint64
	TxHash      common.Hash
	TxIndex     uint32
	Index       uint32
	Address     common.Address
	Topics      []common.Hash
	Data        []byte
}

type BlockData struct {
	Block      Block
	Logs       []Log
	LogSetHash [32]byte
}

// BuildRange validates contiguous headers and matching, duplicate-safe filtered logs.
// The returned slices are owned by the caller and ordered by block/log index.
func BuildRange(from, to uint64, parent common.Hash, blocks []Block, logs []Log) ([]BlockData, error) {
	if from > to || to > math.MaxInt64 || to-from >= uint64(len(blocks))+1 || uint64(len(blocks)) != to-from+1 {
		return nil, ErrInvalidRange
	}
	out := make([]BlockData, len(blocks))
	byHash := make(map[common.Hash]int, len(blocks))
	for i, b := range blocks {
		if b.Number != from+uint64(i) || b.Hash == (common.Hash{}) || b.Time.IsZero() {
			return nil, fmt.Errorf("block %d: %w", i, ErrInvalidRange)
		}
		if b.ParentHash != parent {
			return nil, fmt.Errorf("block %d: %w", b.Number, ErrParent)
		}
		if _, ok := byHash[b.Hash]; ok {
			return nil, fmt.Errorf("block %d: %w", b.Number, ErrIdentity)
		}
		byHash[b.Hash] = i
		out[i].Block = b
		parent = b.Hash
	}
	for _, l := range logs {
		i, ok := byHash[l.BlockHash]
		if !ok || out[i].Block.Number != l.BlockNumber || l.TxHash == (common.Hash{}) || len(l.Topics) > 4 || l.TxIndex > math.MaxInt32 || l.Index > math.MaxInt32 {
			return nil, fmt.Errorf("log at %d: %w", l.BlockNumber, ErrInvalidRange)
		}
		copyLog := l
		copyLog.Topics = append([]common.Hash(nil), l.Topics...)
		copyLog.Data = append([]byte(nil), l.Data...)
		if copyLog.Data == nil {
			copyLog.Data = []byte{}
		}
		out[i].Logs = append(out[i].Logs, copyLog)
	}
	for i := range out {
		sort.Slice(out[i].Logs, func(a, b int) bool { return out[i].Logs[a].Index < out[i].Logs[b].Index })
		unique := out[i].Logs[:0]
		for _, l := range out[i].Logs {
			if len(unique) != 0 && unique[len(unique)-1].Index == l.Index {
				if !EqualLog(unique[len(unique)-1], l) {
					return nil, fmt.Errorf("log index %d: %w", l.Index, ErrIdentity)
				}
				continue
			}
			unique = append(unique, l)
		}
		out[i].Logs = unique
		out[i].LogSetHash = FingerprintLogs(unique)
	}
	return out, nil
}

func EqualLog(a, b Log) bool {
	if a.BlockHash != b.BlockHash || a.BlockNumber != b.BlockNumber || a.TxHash != b.TxHash || a.TxIndex != b.TxIndex || a.Index != b.Index || a.Address != b.Address || len(a.Topics) != len(b.Topics) || !bytes.Equal(a.Data, b.Data) {
		return false
	}
	for i := range a.Topics {
		if a.Topics[i] != b.Topics[i] {
			return false
		}
	}
	return true
}

// FingerprintLogs includes the complete filtered set, including an empty set.
func FingerprintLogs(logs []Log) [32]byte {
	h := sha256.New()
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(logs)))
	h.Write(n[:])
	for _, l := range logs {
		h.Write(l.BlockHash[:])
		binary.BigEndian.PutUint64(n[:], l.BlockNumber)
		h.Write(n[:])
		h.Write(l.TxHash[:])
		binary.BigEndian.PutUint64(n[:], uint64(l.TxIndex))
		h.Write(n[:])
		binary.BigEndian.PutUint64(n[:], uint64(l.Index))
		h.Write(n[:])
		h.Write(l.Address[:])
		binary.BigEndian.PutUint64(n[:], uint64(len(l.Topics)))
		h.Write(n[:])
		for _, topic := range l.Topics {
			h.Write(topic[:])
		}
		binary.BigEndian.PutUint64(n[:], uint64(len(l.Data)))
		h.Write(n[:])
		h.Write(l.Data)
	}
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum
}
