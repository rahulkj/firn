package egress

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func sqliteSinkDSN(t *testing.T, name string) string {
	t.Helper()
	dir := t.TempDir()
	return "file:" + filepath.Join(dir, name) + "?_pragma=busy_timeout(5000)"
}

func TestSqlSinkSQLiteUpsert(t *testing.T) {
	t.Setenv("SQLSINK_TEST_DSN", sqliteSinkDSN(t, "sink.db"))
	dsn := os.Getenv("SQLSINK_TEST_DSN")
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(context.Background(), `CREATE TABLE dest (id integer primary key, note text, qty integer)`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	r := Route{Name: "sql-out"}
	r.Sink = SinkConfig{Type: "sql", Driver: "sqlite", DSNEnv: "SQLSINK_TEST_DSN", Table: "dest", KeyColumns: []string{"id"}}
	sink, err := NewSink(r)
	if err != nil {
		t.Fatal(err)
	}
	batch := []Delivery{
		{Value: []byte(`{"id":1,"note":"a","qty":10}`), Offset: 0, Partition: 0},
		{Value: []byte(`{"id":2,"note":"b","qty":20}`), Offset: 1, Partition: 0},
	}
	if err := sink.Write(context.Background(), "sql-out", batch); err != nil {
		t.Fatal(err)
	}
	// Upsert: same keys, different values → must update, not duplicate.
	batch2 := []Delivery{
		{Value: []byte(`{"id":1,"note":"a2","qty":100}`), Offset: 2, Partition: 0},
	}
	if err := sink.Write(context.Background(), "sql-out", batch2); err != nil {
		t.Fatal(err)
	}
	db2, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db2.Close() }()
	var note string
	var qty int
	var count int
	if err := db2.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM dest").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("row count = %d, want 2 (upsert duplicated instead of updating)", count)
	}
	if err := db2.QueryRowContext(context.Background(), "SELECT note, qty FROM dest WHERE id = 1").Scan(&note, &qty); err != nil {
		t.Fatal(err)
	}
	if note != "a2" || qty != 100 {
		t.Fatalf("upsert did not update: note=%q qty=%d", note, qty)
	}
}

func TestSqlSinkSQLiteAppendOnly(t *testing.T) {
	t.Setenv("SQLSINK_APPEND_DSN", sqliteSinkDSN(t, "append.db"))
	dsn := os.Getenv("SQLSINK_APPEND_DSN")
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(context.Background(), `CREATE TABLE appendlog (id integer primary key, note text)`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	r := Route{Name: "sql-append"}
	r.Sink = SinkConfig{Type: "sql", Driver: "sqlite", DSNEnv: "SQLSINK_APPEND_DSN", Table: "appendlog", AppendOnly: true}
	sink, err := NewSink(r)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		batch := []Delivery{{Value: []byte(fmt.Sprintf(`{"id":%d,"note":"n%d"}`, i+1, i+1)), Offset: int64(i), Partition: 0}}
		if err := sink.Write(context.Background(), "sql-append", batch); err != nil {
			t.Fatal(err)
		}
	}
	db2, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db2.Close() }()
	var count int
	if err := db2.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM appendlog").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("append rows = %d, want 3", count)
	}
}

func TestSqlSinkTypeMapping(t *testing.T) {
	t.Setenv("SQLSINK_MAP_DSN", sqliteSinkDSN(t, "map.db"))
	dsn := os.Getenv("SQLSINK_MAP_DSN")
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(context.Background(), `CREATE TABLE mapped (id integer primary key, flag integer, price real, title text, payload text)`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	r := Route{Name: "sql-map"}
	r.Sink = SinkConfig{Type: "sql", Driver: "sqlite", DSNEnv: "SQLSINK_MAP_DSN", Table: "mapped", KeyColumns: []string{"id"}}
	sink, err := NewSink(r)
	if err != nil {
		t.Fatal(err)
	}
	batch := []Delivery{{Value: []byte(`{"id":1,"flag":true,"price":9.5,"title":"x","payload":{"nested":true}}`), Offset: 0, Partition: 0}}
	if err := sink.Write(context.Background(), "sql-map", batch); err != nil {
		t.Fatal(err)
	}
	db2, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db2.Close() }()
	var flag int
	var price float64
	var payload string
	if err := db2.QueryRowContext(context.Background(), "SELECT flag, price, payload FROM mapped WHERE id=1").Scan(&flag, &price, &payload); err != nil {
		t.Fatal(err)
	}
	if flag != 1 || price != 9.5 {
		t.Fatalf("types wrong: flag=%d price=%v", flag, price)
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(payload), &parsed); err != nil {
		t.Fatalf("payload not JSON: %q (%v)", payload, err)
	}
}

func TestSqlSinkValidation(t *testing.T) {
	r := Route{Name: "x"}
	r.Sink = SinkConfig{Type: "sql", Driver: "sqlite"}
	if _, err := NewSink(r); err == nil {
		t.Fatal("sql without dsn_env must be rejected")
	}
	r.Sink = SinkConfig{Type: "sql", Driver: "sqlite", DSNEnv: "SQLSINK_NONEXIST_ENV_VAR", Table: "t"}
	if _, err := NewSink(r); err == nil {
		t.Fatal("sql with unset dsn env must be rejected")
	}
}

// Live-dialect hooks (only when SQLSINK_TEST_MYSQL / _MSSQL / _ORACLE set):
//   SQLSINK_TEST_MYSQL='firn:testpass@tcp(127.0.0.1:13306)/firn_test'
//   SQLSINK_TEST_MSSQL='sqlserver://sa:YourStrong!Passw0rd@127.0.0.1:1433?database=firn_test'
//   SQLSINK_TEST_ORACLE='oracle://system:oracle@127.0.0.1:1521/FREEPDB1'
// Each expects a table dest(id PK, note VARCHAR, qty INT) to exist.
func TestSqlSinkLiveDialects(t *testing.T) {
	dsnMySQL := os.Getenv("SQLSINK_TEST_MYSQL")
	dsnMSSQL := os.Getenv("SQLSINK_TEST_MSSQL")
	dsnOracle := os.Getenv("SQLSINK_TEST_ORACLE")
	if dsnMySQL == "" && dsnMSSQL == "" && dsnOracle == "" {
		t.Skip("no live dialects configured")
	}
	for name, cfg := range map[string]struct{ driver, dsn, table string }{
		"mysql":  {"mysql", dsnMySQL, "dest"},
		"mssql":  {"sqlserver", dsnMSSQL, "dest"},
		"oracle": {"oracle", dsnOracle, "dest"},
	} {
		if cfg.dsn == "" {
			continue
		}
		t.Setenv("SQLSINK_LIVE_DSN", cfg.dsn)
		r := Route{Name: "live-" + name}
		r.Sink = SinkConfig{Type: "sql", Driver: cfg.driver, DSNEnv: "SQLSINK_LIVE_DSN", Table: cfg.table, KeyColumns: []string{"id"}}
		sink, err := NewSink(r)
		if err != nil {
			t.Fatal(err)
		}
		batch := []Delivery{
			{Value: []byte(`{"id":900001,"note":"conn","qty":1}`), Offset: 0, Partition: 0},
		}
		if err := sink.Write(context.Background(), "live-"+name, batch); err != nil {
			t.Fatalf("%s upsert: %v", name, err)
		}
		// upsert again
		batch2 := []Delivery{{Value: []byte(`{"id":900001,"note":"conn2","qty":2}`), Offset: 1, Partition: 0}}
		if err := sink.Write(context.Background(), "live-"+name, batch2); err != nil {
			t.Fatalf("%s re-upsert: %v", name, err)
		}
	}
}
