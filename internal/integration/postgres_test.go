package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/jackc/pgx/v5/pgconn"
)

// Live Postgres CDC test. Requires PG_CDC_TEST_DSN pointing at a database
// with wal_level=logical, e.g.
//   PG_CDC_TEST_DSN=postgres://USER:PASS@HOST:5432/dbname?sslmode=disable go test ./internal/integration/ -run TestPostgresCDC -v
// Skips silently when unset.
func TestPostgresCDCLive(t *testing.T) {
	dsn := envOrSkip(t, "PG_CDC_TEST_DSN")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(ctx, "DROP TABLE IF EXISTS cdc_test").ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "CREATE TABLE cdc_test (id serial primary key, note text NOT NULL, qty int)").ReadAll(); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "ALTER TABLE cdc_test REPLICA IDENTITY FULL").ReadAll(); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close(context.Background())

	// Create the connector (and its slot) BEFORE the rows exist; the slot
	// streams from its creation point.
	cdc, err := NewPostgresCDCConnector("pg-test", "PG_CDC_TEST_DSN", "firn_test_slot")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cdc.Close() }()

	conn2, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn2.Exec(ctx, "INSERT INTO cdc_test (note, qty) VALUES ('a', 1), ('b', 2), ('c', 3)"); err != nil {
		t.Fatal(err)
	}
	_ = conn2.Close(ctx)

	position := ""
	seen := map[string]int{}
	fmtPosition := func() string { return position }
	for range 50 {
		recs, err := cdc.FetchBatch(ctx, fmtPosition(), 10)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			var m map[string]any
			if err := json.Unmarshal(r.Value, &m); err != nil {
				t.Fatal(err)
			}
			tbl, _ := m["table"].(string)
			if tbl != "cdc_test" {
				t.Fatalf("unexpected table %q: %s", tbl, r.Value)
			}
			seen[fmt.Sprint(m["op"])]++
			if r.Position == "" {
				t.Fatal("record without position")
			}
			position = r.Position
		}
		if len(seen) >= 1 && seen["c"] >= 3 {
			break
		}
	}
	if seen["c"] != 3 {
		t.Fatalf("inserts seen = %d, want 3 (all: %v)", seen["c"], seen)
	}

	// Update + delete.
	conn3, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn3.Exec(ctx, "UPDATE cdc_test SET qty = 100 WHERE note = 'a'"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn3.Exec(ctx, "DELETE FROM cdc_test WHERE note = 'b'"); err != nil {
		t.Fatal(err)
	}
	_ = conn3.Close(ctx)

	seen2 := map[string]int{}
	for range 50 {
		recs, err := cdc.FetchBatch(ctx, position, 10)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			var m map[string]any
			_ = json.Unmarshal(r.Value, &m)
			seen2[fmt.Sprint(m["op"])]++
			position = r.Position
		}
		if seen2["u"] >= 1 && seen2["d"] >= 1 {
			break
		}
	}
	if seen2["u"] != 1 || seen2["d"] != 1 {
		t.Fatalf("update/delete seen = %v, want 1/1", seen2)
	}

	// Resume from the checkpoint: nothing new.
	for range 20 {
		recs, err := cdc.FetchBatch(ctx, position, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(recs) == 0 {
			return // clean drain — checkpoint works
		}
		position = recs[len(recs)-1].Position
	}
	t.Fatal("expected drained source after checkpoint resume")
}

func envOrSkip(t *testing.T, key string) string {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		t.Skipf("%s not set; skipping live Postgres CDC test", key)
	}
	return v
}
