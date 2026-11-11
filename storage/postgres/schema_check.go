package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/runnerq/runnerq-go/internal/spec"
)

// No-op DDL still locks: ADD COLUMN IF NOT EXISTS takes ACCESS EXCLUSIVE and
// CREATE INDEX IF NOT EXISTS takes SHARE before checking IF NOT EXISTS, so each
// process start would stall live claims/acks/parks elsewhere and risk being a
// deadlock victim. initSchema first checks the catalog and skips DDL entirely
// when current. The expectation is runnerq-spec's catalog: what the migrations
// and concurrent indexes produce.
type schemaExpectation struct {
	tables         []string
	columns        []string // "table.column"
	indexes        []string // must be valid
	retired        []string // replaced concurrent indexes (must be gone)
	retiredColumns []string // "table.column"; the migrations move their data and drop them
}

var expectedSchema = sync.OnceValue(func() schemaExpectation {
	c := spec.PostgresCatalog
	e := schemaExpectation{retired: c.RetiredIndexes, retiredColumns: c.RetiredColumns}
	for _, t := range c.Tables {
		e.tables = append(e.tables, t.Name)
		for _, col := range t.Columns {
			e.columns = append(e.columns, t.Name+"."+col.Name)
		}
	}
	for _, idx := range c.Indexes {
		e.indexes = append(e.indexes, idx.Name)
	}
	return e
})

// schemaCurrent checks the connection's current schema in one catalog round
// trip, taking no table locks.
func schemaCurrent(ctx context.Context, conn *pgxpool.Conn) (bool, error) {
	e := expectedSchema()
	var current bool
	err := conn.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM information_schema.tables
		        WHERE table_schema = current_schema() AND table_name = ANY($1)) = cardinality($1::text[])
		   AND (SELECT count(*) FROM information_schema.columns
		        WHERE table_schema = current_schema() AND table_name || '.' || column_name = ANY($2)) = cardinality($2::text[])
		   AND (SELECT count(*) FROM pg_index i
		        JOIN pg_class c ON c.oid = i.indexrelid
		        JOIN pg_namespace n ON n.oid = c.relnamespace
		        WHERE n.nspname = current_schema() AND c.relname = ANY($3) AND i.indisvalid) = cardinality($3::text[])
		   AND NOT EXISTS (SELECT 1 FROM pg_class c
		        JOIN pg_namespace n ON n.oid = c.relnamespace
		        WHERE n.nspname = current_schema() AND c.relname = ANY($4))
		   AND NOT EXISTS (SELECT 1 FROM information_schema.columns
		        WHERE table_schema = current_schema() AND table_name || '.' || column_name = ANY($5))`,
		e.tables, e.columns, e.indexes, e.retired, e.retiredColumns).Scan(&current)
	if err != nil {
		return false, databaseError(err, fmt.Sprintf("Failed to inspect schema: %v", err))
	}
	return current, nil
}

// Schema DDL against live traffic (rolling deploy, parallel test packages) can
// be chosen as a deadlock victim; the DDL is idempotent, so it is re-run.
func isDeadlock(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "40P01"
}

func retryDeadlock(ctx context.Context, what string, op func() error) error {
	const attempts = 5
	var err error
	for i := range attempts {
		if err = op(); err == nil || !isDeadlock(err) {
			return err
		}
		slog.Warn("runnerq: schema statement lost a deadlock; retrying", "step", what, "attempt", i+1)
		select {
		case <-time.After(time.Duration(50<<i) * time.Millisecond):
		case <-ctx.Done():
			return err
		}
	}
	return err
}
