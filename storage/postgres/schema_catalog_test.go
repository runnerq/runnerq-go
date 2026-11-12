package postgres

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/runnerq/runnerq-go/internal/spec"
	"github.com/runnerq/runnerq-go/internal/spectest"
)

var (
	indexSchemaRe = regexp.MustCompile(`\bon\s+(?:"(?:[^"]|"")+"|[a-z_]\w*)\.`)
	indexCastRe   = regexp.MustCompile(`::text(?:\[\])?`)
	indexAnyRe    = regexp.MustCompile(`=\s*any\s*\(\s*array\s*\[`)
	indexAscRe    = regexp.MustCompile(`\s+asc\b`)
	indexStripRe  = regexp.MustCompile(`[\s"()\[\];]`)
	defaultCastRe = regexp.MustCompile(`::(?:text|integer|bigint|smallint)`)
	defaultStrip  = regexp.MustCompile(`[\s()]`)
)

// normalizeIndex and normalizeDefault are the spec's catalog normalization
// (vectors/index_definition.json, vectors/column_default.json).
func normalizeIndex(def string) string {
	s := strings.ToLower(def)
	s = indexSchemaRe.ReplaceAllString(s, "on ")
	s = indexCastRe.ReplaceAllString(s, "")
	s = indexAnyRe.ReplaceAllString(s, "in(")
	s = strings.ReplaceAll(s, "using btree", "")
	s = indexAscRe.ReplaceAllString(s, "")
	return indexStripRe.ReplaceAllString(s, "")
}

func normalizeDefault(def *string) *string {
	if def == nil {
		return nil
	}
	s := defaultStrip.ReplaceAllString(defaultCastRe.ReplaceAllString(strings.ToLower(*def), ""), "")
	if strings.HasPrefix(s, "nextval") {
		s = "nextval"
	}
	return &s
}

func TestSpecIndexDefinition(t *testing.T) {
	type in struct {
		Definition string `json:"definition"`
	}
	for _, c := range spectest.Load[in, string](t, "vectors/index_definition.json") {
		if got := normalizeIndex(c.Input.Definition); got != c.Output {
			t.Errorf("%s: got %s, want %s", c.Name, got, c.Output)
		}
	}
}

func TestSpecColumnDefault(t *testing.T) {
	type in struct {
		Default *string `json:"default"`
	}
	for _, c := range spectest.Load[in, *string](t, "vectors/column_default.json") {
		got := normalizeDefault(c.Input.Default)
		if (got == nil) != (c.Output == nil) || (got != nil && *got != *c.Output) {
			t.Errorf("%s: got %v, want %v", c.Name, got, c.Output)
		}
	}
}

// catalogDiff compares the connection's current schema with the spec's
// catalog, both ways, normalized as the spec says.
func catalogDiff(ctx context.Context, conn *pgx.Conn) ([]string, error) {
	want := map[string]string{}
	for _, tb := range spec.PostgresCatalog.Tables {
		want["primary key "+tb.Name] = strings.Join(tb.PrimaryKey, ",")
		for _, col := range tb.Columns {
			var def *string
			if col.Default != "" {
				def = &col.Default
			}
			want["column "+tb.Name+"."+col.Name] = columnKey(col.Type, col.Nullable, def)
		}
	}
	for _, idx := range spec.PostgresCatalog.Indexes {
		want["index "+idx.Name] = idx.Table + " " + normalizeIndex(idx.Definition)
	}

	got := map[string]string{}
	rows, err := conn.Query(ctx, `SELECT table_name, column_name, udt_name, is_nullable = 'YES', column_default
		FROM information_schema.columns WHERE table_schema = current_schema()`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var table, column, typ string
		var nullable bool
		var def *string
		if err := rows.Scan(&table, &column, &typ, &nullable, &def); err != nil {
			return nil, err
		}
		got["column "+table+"."+column] = columnKey(typ, nullable, def)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows, err = conn.Query(ctx, `SELECT c.relname, array_agg(a.attname::text ORDER BY k.ordinality)
		FROM pg_constraint p
		JOIN pg_class c ON c.oid = p.conrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		CROSS JOIN LATERAL unnest(p.conkey) WITH ORDINALITY k(num, ordinality)
		JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.num
		WHERE p.contype = 'p' AND n.nspname = current_schema() GROUP BY c.relname`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var table string
		var key []string
		if err := rows.Scan(&table, &key); err != nil {
			return nil, err
		}
		got["primary key "+table] = strings.Join(key, ",")
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows, err = conn.Query(ctx, `SELECT c.relname, t.relname, pg_get_indexdef(c.oid), i.indisvalid
		FROM pg_index i
		JOIN pg_class c ON c.oid = i.indexrelid
		JOIN pg_class t ON t.oid = i.indrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = current_schema() AND NOT i.indisprimary`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var name, table, def string
		var valid bool
		if err := rows.Scan(&name, &table, &def, &valid); err != nil {
			return nil, err
		}
		key := table + " " + normalizeIndex(def)
		if !valid {
			key += " (invalid)"
		}
		got["index "+name] = key
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var diffs []string
	for k, w := range want {
		if g, ok := got[k]; !ok {
			diffs = append(diffs, "missing "+k)
		} else if g != w {
			diffs = append(diffs, fmt.Sprintf("%s: database has %q, spec has %q", k, g, w))
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			diffs = append(diffs, "not in the spec: "+k)
		}
	}
	slices.Sort(diffs)
	return diffs, nil
}

func columnKey(typ string, nullable bool, def *string) string {
	d := "none"
	if n := normalizeDefault(def); n != nil {
		d = *n
	}
	return fmt.Sprintf("%s nullable=%v default=%s", typ, nullable, d)
}

// A database the backend migrates is exactly the spec's catalog: nothing
// missing, nothing extra. A fresh schema keeps other tests' objects out.
func TestSchemaMatchesSpecCatalog(t *testing.T) {
	dsn := os.Getenv("RUNNERQ_TEST_DSN")
	if dsn == "" {
		t.Skip("RUNNERQ_TEST_DSN not set; skipping integration test")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	schema := "catalog_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE") })

	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	b, err := WithConfig(ctx, u.String(), "catalog_check", 30_000, 2)
	if err != nil {
		t.Fatal(err)
	}
	b.Close()

	conn, err := pgx.Connect(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	diffs, err := catalogDiff(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	if len(diffs) > 0 {
		t.Fatalf("migrated schema differs from the spec catalog:\n  %s", strings.Join(diffs, "\n  "))
	}
}
