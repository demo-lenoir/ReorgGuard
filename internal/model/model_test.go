package model

import (
	"errors"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

func hash(b byte) common.Hash { var h common.Hash; h[31] = b; return h }
func fixture() ([]Block, []Log) {
	blocks := []Block{{Number: 1, Hash: hash(1), ParentHash: hash(9), Time: time.Unix(1, 0).UTC()}, {Number: 2, Hash: hash(2), ParentHash: hash(1), Time: time.Unix(2, 0).UTC()}}
	logs := []Log{{BlockHash: hash(1), BlockNumber: 1, TxHash: hash(3), Index: 0, Address: common.Address{19: 1}, Data: []byte{1}}}
	return blocks, logs
}
func TestBuildRangeIdentity(t *testing.T) {
	blocks, logs := fixture()
	got, err := BuildRange(1, 2, hash(9), blocks, append(logs, logs[0]))
	if err != nil || len(got) != 2 || len(got[0].Logs) != 1 || len(got[1].Logs) != 0 {
		t.Fatalf("duplicate normalization: %#v %v", got, err)
	}
	if got[0].LogSetHash == got[1].LogSetHash {
		t.Fatal("distinct complete log sets have same digest")
	}
	changed := logs[0]
	changed.Data = []byte{2}
	if _, err = BuildRange(1, 2, hash(9), blocks, append(logs, changed)); !errors.Is(err, ErrIdentity) {
		t.Fatalf("changed duplicate: %v", err)
	}
	blocks[1].ParentHash = hash(8)
	if _, err = BuildRange(1, 2, hash(9), blocks, logs); !errors.Is(err, ErrParent) {
		t.Fatalf("bad parent: %v", err)
	}
}
func TestBuildRangeRejectsMismatchedLog(t *testing.T) {
	blocks, logs := fixture()
	logs[0].BlockNumber = 2
	if _, err := BuildRange(1, 2, hash(9), blocks, logs); !errors.Is(err, ErrInvalidRange) {
		t.Fatalf("mismatched log: %v", err)
	}
}
func FuzzBuildRange(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		blocks, logs := fixture()
		if len(data) > 0 {
			blocks[0].Hash[31] = data[0]
		}
		if len(data) > 1 {
			blocks[1].ParentHash[31] = data[1]
		}
		if len(data) > 2 {
			logs[0].BlockHash[31] = data[2]
		}
		if len(data) > 3 {
			logs[0].Data = append([]byte(nil), data[3:]...)
		}
		items, err := BuildRange(1, 2, hash(9), blocks, logs)
		if err == nil && (len(items) != 2 || items[0].Block.Number != 1 || items[1].Block.Number != 2 || items[0].Block.ParentHash != hash(9)) {
			t.Fatal("accepted invalid range")
		}
	})
}
