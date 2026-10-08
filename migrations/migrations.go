package migrations

import (
	"context"
	"embed"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed 001_init.sql 002_operational_api.sql
var files embed.FS

func Apply(ctx context.Context, db *pgxpool.Pool) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration: %w", err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(734215911)"); err != nil {
		return fmt.Errorf("lock migration: %w", err)
	}
	if _, err = tx.Exec(ctx, "CREATE TABLE IF NOT EXISTS schema_migrations (version integer PRIMARY KEY)"); err != nil {
		return fmt.Errorf("create migration ledger: %w", err)
	}
	for version, name := range []string{"001_init.sql", "002_operational_api.sql"} {
		v := version + 1
		var present bool
		if err = tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version=$1)", v).Scan(&present); err != nil {
			return fmt.Errorf("read migration ledger: %w", err)
		}
		if present {
			continue
		}
		sql, readErr := files.ReadFile(name)
		if readErr != nil {
			return fmt.Errorf("read migration %d: %w", v, readErr)
		}
		if _, err = tx.Exec(ctx, string(sql), pgx.QueryExecModeSimpleProtocol); err != nil {
			return fmt.Errorf("apply migration %d: %w", v, err)
		}
		if _, err = tx.Exec(ctx, "INSERT INTO schema_migrations(version) VALUES ($1)", v); err != nil {
			return fmt.Errorf("record migration %d: %w", v, err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration: %w", err)
	}
	return nil
}
