//go:build integration

package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"reorgguard/internal/model"
)

var schemaID atomic.Uint64

func testStore(t *testing.T) (*Store, *pgxpool.Config) {
	t.Helper()
	dsn := os.Getenv("REORGGUARD_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("REORGGUARD_TEST_DATABASE_URL required for integration tests")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("rg_test_%d_%d", time.Now().UnixNano(), schemaID.Add(1))
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	s, err := OpenConfig(ctx, cfg, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		s.Close()
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		admin.Close(context.Background())
	})
	return s, cfg
}
func testHash(n byte) common.Hash { var h common.Hash; h[31] = n; return h }
func testBlock(n uint64) model.Block {
	p := common.Hash{}
	if n > 0 {
		p = testHash(byte(n))
	}
	return model.Block{Number: n, Hash: testHash(byte(n + 1)), ParentHash: p, Time: time.Unix(int64(n+1), 0).UTC()}
}
func testDataset() Dataset {
	return Dataset{ChainID: 31337, Genesis: testHash(1), FilterHash: [32]byte{1}, StartBlock: 1, Anchor: testBlock(0)}
}
func testBatch(t *testing.T, from, to uint64, withLog bool) []model.BlockData {
	t.Helper()
	blocks := make([]model.Block, 0, to-from+1)
	for n := from; n <= to; n++ {
		blocks = append(blocks, testBlock(n))
	}
	var logs []model.Log
	if withLog {
		logs = []model.Log{{BlockHash: blocks[0].Hash, BlockNumber: from, TxHash: testHash(90), Index: 0, Address: common.Address{19: 1}, Topics: []common.Hash{testHash(91)}, Data: []byte{1}}}
	}
	items, err := model.BuildRange(from, to, blocks[0].ParentHash, blocks, logs)
	if err != nil {
		t.Fatal(err)
	}
	return items
}
func counts(t *testing.T, s *Store) (int64, int64) {
	t.Helper()
	var blocks, logs int64
	if err := s.db.QueryRow(context.Background(), "SELECT count(*) FROM blocks WHERE canonical").Scan(&blocks); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(context.Background(), "SELECT count(*) FROM logs").Scan(&logs); err != nil {
		t.Fatal(err)
	}
	return blocks, logs
}
func TestPostgresAppendReplayIdentityAndRestart(t *testing.T) {
	s, cfg := testStore(t)
	ctx := context.Background()
	cp, err := s.Init(ctx, testDataset())
	if err != nil || cp.Number != 0 {
		t.Fatalf("init %+v %v", cp, err)
	}
	items := testBatch(t, 1, 2, true)
	if err = s.Append(ctx, 31337, items); err != nil {
		t.Fatal(err)
	}
	if err = s.Append(ctx, 31337, items); err != nil {
		t.Fatalf("exact replay: %v", err)
	}
	b, l := counts(t, s)
	if b != 3 || l != 1 {
		t.Fatalf("duplicate rows: %d blocks %d logs", b, l)
	}
	mutated := testBatch(t, 1, 2, true)
	mutated[0].Block.Time = mutated[0].Block.Time.Add(time.Second)
	if err = s.Append(ctx, 31337, mutated); !errors.Is(err, model.ErrIdentity) {
		t.Fatalf("changed block: %v", err)
	}
	mutated = testBatch(t, 1, 2, true)
	mutated[0].Logs[0].Data = []byte{9}
	mutated[0].LogSetHash = model.FingerprintLogs(mutated[0].Logs)
	if err = s.Append(ctx, 31337, mutated); !errors.Is(err, model.ErrIdentity) {
		t.Fatalf("changed log: %v", err)
	}
	if cp, err = s.Checkpoint(ctx, 31337); err != nil || cp.Number != 2 {
		t.Fatalf("checkpoint %+v %v", cp, err)
	}
	// Reopen the repository against the same schema, then continue at exactly 3.
	reopened, err := OpenConfig(ctx, cfg, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	cp, err = reopened.Init(ctx, testDataset())
	if err != nil || cp.Number != 2 {
		t.Fatalf("reopen %+v %v", cp, err)
	}
	if err = reopened.Append(ctx, 31337, testBatch(t, 3, 3, false)); err != nil {
		t.Fatal(err)
	}
	b, l = counts(t, s)
	if b != 4 || l != 1 {
		t.Fatalf("restart rows: %d blocks %d logs", b, l)
	}
	if cp, err = reopened.Checkpoint(ctx, 31337); err != nil || cp.Number != 3 {
		t.Fatalf("checkpoint %+v %v", cp, err)
	}
}
func TestPostgresFailureBeforeCommitRollsBack(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	if _, err := s.Init(ctx, testDataset()); err != nil {
		t.Fatal(err)
	}
	s.beforeCommit = func(context.Context, pgx.Tx) error { return errors.New("injected failure before COMMIT") }
	err := s.Append(ctx, 31337, testBatch(t, 1, 2, true))
	if err == nil {
		t.Fatal("expected failure")
	}
	cp, _ := s.Checkpoint(ctx, 31337)
	b, l := counts(t, s)
	if cp.Number != 0 || b != 1 || l != 0 {
		t.Fatalf("partial write: cp=%d blocks=%d logs=%d", cp.Number, b, l)
	}
	s.beforeCommit = nil
	if err = s.Append(ctx, 31337, testBatch(t, 1, 2, true)); err != nil {
		t.Fatal(err)
	}
}
func TestPostgresRejectsChangedCompletedLogSet(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	if _, err := s.Init(ctx, testDataset()); err != nil {
		t.Fatal(err)
	}
	items := testBatch(t, 1, 1, true)
	if err := s.Append(ctx, 31337, items); err != nil {
		t.Fatal(err)
	}
	blockHash, txHash := testHash(2), testHash(92)
	address := common.Address{19: 1}
	_, err := s.db.Exec(ctx, `INSERT INTO logs(chain_id,block_hash,block_number,tx_hash,tx_index,log_index,address,topics,data)
		VALUES(31337,$1,1,$2,0,1,$3,$4,$5)`, blockHash[:], txHash[:], address[:], [][]byte{}, []byte{2})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Append(ctx, 31337, items); !errors.Is(err, model.ErrIdentity) {
		t.Fatalf("changed complete set: %v", err)
	}
	cp, _ := s.Checkpoint(ctx, 31337)
	if cp.Number != 1 {
		t.Fatalf("checkpoint changed: %+v", cp)
	}
}
func TestPostgresFailureDuringTransactionRollsBack(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	if _, err := s.Init(ctx, testDataset()); err != nil {
		t.Fatal(err)
	}
	_, err := s.db.Exec(ctx, `CREATE FUNCTION reject_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected checkpoint failure'; END $$`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.db.Exec(ctx, `CREATE TRIGGER reject_checkpoint BEFORE UPDATE ON sync_state FOR EACH ROW EXECUTE FUNCTION reject_checkpoint()`)
	if err != nil {
		t.Fatal(err)
	}
	err = s.Append(ctx, 31337, testBatch(t, 1, 2, true))
	if err == nil {
		t.Fatal("expected trigger failure")
	}
	cp, _ := s.Checkpoint(ctx, 31337)
	b, l := counts(t, s)
	if cp.Number != 0 || b != 1 || l != 0 {
		t.Fatalf("partial transaction: cp=%d blocks=%d logs=%d", cp.Number, b, l)
	}
}
func TestPostgresContextCancellationAtLock(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	if _, err := s.Init(ctx, testDataset()); err != nil {
		t.Fatal(err)
	}
	blocker, err := s.db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(ctx)
	if _, err = blocker.Exec(ctx, "UPDATE sync_state SET updated_at=now() WHERE chain_id=31337"); err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
	defer cancel()
	err = s.Append(short, 31337, testBatch(t, 1, 1, true))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancel DB work: %v", err)
	}
	cp, _ := s.Checkpoint(ctx, 31337)
	b, l := counts(t, s)
	if cp.Number != 0 || b != 1 || l != 0 {
		t.Fatalf("canceled write: cp=%d blocks=%d logs=%d", cp.Number, b, l)
	}
}
func TestPostgresConstraintsAndDatasetIdentity(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	if _, err := s.Init(ctx, testDataset()); err != nil {
		t.Fatal(err)
	}
	d := testDataset()
	d.FilterHash[0] = 2
	if _, err := s.Init(ctx, d); !errors.Is(err, ErrConfigMismatch) {
		t.Fatalf("changed filter: %v", err)
	}
	otherHash, otherParent := testHash(88), testHash(0)
	_, err := s.db.Exec(ctx, `INSERT INTO blocks(chain_id,number,hash,parent_hash,block_time,canonical,logs_complete)
		VALUES(31337,0,$1,$2,now(),true,false)`, otherHash[:], otherParent[:])
	if err == nil {
		t.Fatal("canonical height uniqueness not enforced")
	}
	if err = s.Append(ctx, 31337, testBatch(t, 1, 1, true)); err != nil {
		t.Fatal(err)
	}
	blockHash := testHash(2)
	if _, err = s.db.Exec(ctx, "UPDATE blocks SET parent_hash=$1 WHERE chain_id=31337 AND hash=$2", otherHash[:], blockHash[:]); err == nil {
		t.Fatal("block identity update accepted")
	}
	if _, err = s.db.Exec(ctx, "UPDATE logs SET data=$1 WHERE chain_id=31337 AND block_hash=$2", []byte{9}, blockHash[:]); err == nil {
		t.Fatal("log identity update accepted")
	}
	if _, err = s.db.Exec(ctx, "DELETE FROM logs WHERE chain_id=31337 AND block_hash=$1", blockHash[:]); err == nil {
		t.Fatal("log deletion accepted")
	}
	emptyDigest := model.FingerprintLogs(nil)
	if _, err = s.db.Exec(ctx, `INSERT INTO blocks(chain_id,number,hash,parent_hash,block_time,canonical,logs_complete,log_count,log_set_hash)
		VALUES(31337,2,$1,$2,now(),false,true,0,$3)`, otherHash[:], blockHash[:], emptyDigest[:]); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(ctx, "UPDATE sync_state SET checkpoint_number=2,checkpoint_hash=$1 WHERE chain_id=31337", otherHash[:]); err == nil {
		t.Fatal("noncanonical checkpoint accepted")
	}
}
