package integration

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"

	"database/sql"
)

func sqliteDSN(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	return "file:" + path + "?_pragma=busy_timeout(5000)"
}

func seedSQLite(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	_, err = db.ExecContext(context.Background(), `CREATE TABLE items (id integer primary key, note text, qty integer, updated_at text)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(context.Background(), `INSERT INTO items (id, note, qty, updated_at) VALUES
		(1, 'a', 10, '2026-11-01T00:00:01Z'),
		(2, 'b', 20, '2026-11-02T00:00:02Z'),
		(3, 'c', 30, '2026-11-03T00:00:03Z')`)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSqlPollingSourceInt64Watermark(t *testing.T) {
	t.Setenv("SQL_TEST_DSN", sqliteDSN(t))
	dsn := os.Getenv("SQL_TEST_DSN")
	seedSQLite(t, dsn)

	conn, err := NewSqlPollingConnector("sql-test", "sqlite", "SQL_TEST_DSN", "items", "id", "int64", []string{"id"}, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	ctx := context.Background()
	recs, err := conn.FetchBatch(ctx, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 {
		t.Fatalf("got %d records, want 3", len(recs))
	}
	var m map[string]any
	_ = json.Unmarshal(recs[0].Value, &m)
	if m["note"] != "a" || m["id"].(float64) != 1 {
		t.Fatalf("row 0 wrong: %v", m)
	}
	if recs[0].Position != "1" || recs[2].Position != "3" {
		t.Fatalf("positions %q %q", recs[0].Position, recs[2].Position)
	}
	// Resume from position 2 → only id=3.
	recs2, err := conn.FetchBatch(ctx, recs[1].Position, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs2) != 1 || recs2[0].Position != "3" {
		t.Fatalf("resumed batch = %v", recs2)
	}
	// And again → drained.
	recs3, err := conn.FetchBatch(ctx, recs2[0].Position, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs3) != 0 {
		t.Fatalf("drained batch = %d records", len(recs3))
	}
}

func TestSqlPollingSourceTimestampWatermark(t *testing.T) {
	t.Setenv("SQL_TEST_DSN", sqliteDSN(t))
	dsn := os.Getenv("SQL_TEST_DSN")
	seedSQLite(t, dsn)
	conn, err := NewSqlPollingConnector("sql-ts", "sqlite", "SQL_TEST_DSN", "items", "updated_at", "timestamp", []string{"id"}, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	ctx := context.Background()
	recs, err := conn.FetchBatch(ctx, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range recs {
		t.Logf("rec %d position=%q value=%s", i, r.Position, r.Value)
	}
	if len(recs) != 3 {
		t.Fatalf("got %d records", len(recs))
	}
	recs2, err := conn.FetchBatch(ctx, recs[1].Position, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs2) != 1 {
		t.Fatalf("resumed batch = %d", len(recs2))
	}
}

func TestSqlPollingConnectorValidation(t *testing.T) {
	if _, err := NewSqlPollingConnector("bad name!", "sqlite", "SQL_TEST_DSN", "items", "id", "int64", nil, 0); err == nil {
		t.Fatal("bad name accepted")
	}
	if _, err := NewSqlPollingConnector("ok", "nosuchdb", "SQL_TEST_DSN", "items", "id", "int64", nil, 0); err == nil {
		t.Fatal("bad driver accepted")
	}
	if _, err := NewSqlPollingConnector("ok", "sqlite", "", "items", "id", "int64", nil, 0); err == nil {
		t.Fatal("missing dsn_env accepted")
	}
	if _, err := NewSqlPollingConnector("ok", "sqlite", "SQL_TEST_DSN", "items; DROP TABLE items", "id", "int64", nil, 0); err == nil {
		t.Fatal("injection table accepted")
	}
	if _, err := NewSqlPollingConnector("ok", "sqlite", "SQL_TEST_DSN", "items", "id", "micro", nil, 0); err == nil {
		t.Fatal("bad watermark type accepted")
	}
	// Dotted identifiers OK
	if _, err := NewSqlPollingConnector("ok", "sqlite", "SQL_TEST_DSN", "main.items", "id", "int64", []string{"id"}, 0); err != nil {
		t.Fatalf("dotted table rejected: %v", err)
	}
}
