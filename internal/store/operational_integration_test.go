//go:build integration

package store

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"reorgguard/internal/model"
)

func TestOperationalPageRevisionAndReorgHistory(t *testing.T) {
	s, cfg := testStore(t)
	ctx := context.Background()
	if _, err := s.Init(ctx, testDataset()); err != nil {
		t.Fatal(err)
	}
	blocks := []model.Block{testBlock(1), testBlock(2), testBlock(3)}
	logs := make([]model.Log, 3)
	for i, b := range blocks {
		logs[i] = model.Log{BlockHash: b.Hash, BlockNumber: b.Number, TxHash: testHash(byte(90 + i)), Index: 0, Address: common.Address{19: 1}, Topics: []common.Hash{testHash(91)}, Data: []byte{byte(i)}}
	}
	items, err := model.BuildRange(1, 3, testBlock(0).Hash, blocks, logs)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Append(ctx, 31337, items); err != nil {
		t.Fatal(err)
	}
	q := LogQuery{From: 1, To: 3, Limit: 2}
	first, err := s.ListLogs(ctx, 31337, q)
	if err != nil || len(first.Items) != 2 || first.Next == nil {
		t.Fatalf("first %+v %v", first, err)
	}
	q.After = first.Next
	q.Revision = &first.Revision
	second, err := s.ListLogs(ctx, 31337, q)
	if err != nil || len(second.Items) != 1 || second.Next != nil || second.Items[0].BlockNumber != 3 {
		t.Fatalf("second %+v %v", second, err)
	}
	b3 := model.Block{Number: 3, Hash: testHash(99), ParentHash: testBlock(2).Hash, Time: time.Unix(99, 0).UTC()}
	replacement, err := model.BuildRange(3, 3, b3.ParentHash, []model.Block{b3}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Reconcile(ctx, 31337, Checkpoint{Number: 3, Hash: testBlock(3).Hash}, Checkpoint{Number: 2, Hash: testBlock(2).Hash}, replacement, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ListLogs(ctx, 31337, q); !errors.Is(err, ErrRevisionChanged) {
		t.Fatalf("stale cursor: %v", err)
	}
	snap, err := s.Snapshot(ctx, 31337)
	if err != nil || snap.Revision != 1 || snap.Checkpoint.Hash != b3.Hash || len(snap.LastReorg) == 0 {
		t.Fatalf("snapshot %+v %v", snap, err)
	}
	page, err := s.ListLogs(ctx, 31337, LogQuery{From: 1, To: 3, Limit: 100})
	if err != nil || len(page.Items) != 2 {
		t.Fatalf("canonical page %+v %v", page, err)
	}
	old, err := s.BlockByHash(ctx, 31337, testBlock(3).Hash)
	if err != nil || old.Canonical {
		t.Fatalf("orphan %+v %v", old, err)
	}
	reopened, err := OpenConfig(ctx, cfg, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	snap, err = reopened.Snapshot(ctx, 31337)
	if err != nil || snap.Revision != 1 || len(snap.LastReorg) == 0 {
		t.Fatalf("reopen %+v %v", snap, err)
	}
}

func TestOperationalOversizedPayloadIsStoredButNotServed(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	if _, err := s.Init(ctx, testDataset()); err != nil {
		t.Fatal(err)
	}
	b := testBlock(1)
	log := model.Log{BlockHash: b.Hash, BlockNumber: 1, TxHash: testHash(88), Index: 0, Address: common.Address{19: 1}, Data: bytes.Repeat([]byte{7}, MaxAPIDataBytes+1)}
	items, err := model.BuildRange(1, 1, testBlock(0).Hash, []model.Block{b}, []model.Log{log})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Append(ctx, 31337, items); err != nil {
		t.Fatal(err)
	}
	_, err = s.ListLogs(ctx, 31337, LogQuery{From: 1, To: 1, Limit: 1})
	if !errors.Is(err, ErrPageTooLarge) {
		t.Fatalf("page limit: %v", err)
	}
	stored, err := s.CanonicalLogs(ctx, 31337, 1, 1)
	if err != nil || len(stored) != 1 || len(stored[0].Data) != MaxAPIDataBytes+1 {
		t.Fatalf("durable log lost: %v %+v", err, stored)
	}
}
