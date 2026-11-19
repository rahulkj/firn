package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	_ "modernc.org/sqlite"
	_ "github.com/jackc/pgx/v5/stdlib"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/microsoft/go-mssqldb"
	_ "github.com/sijms/go-ora/v2"
)

// SqlPollingConnector is the universal RDBMS source: it polls a table (or
// query) ordered by a monotonically increasing watermark column and emits
// one record per row. Works on ANY database/sql driver; no per-dialect
// plugins, no WAL/binlog dependency. Chained migration use case:
//
//   legacy Oracle/SQL Server/MySQL table → polling watermark → Firn → anywhere
//
// Records are full rows as JSON objects, keyed by key_columns (JSON of the
// key values). Position = watermark value (decimal int64 or RFC 3339Nano
// UTC); rows are read WHERE watermark > :last, ORDER BY watermark ASC.
// Delivery is at-least-once; downstream dedupe by key is expected.
type SqlPollingConnector struct {
	name       string
	driver     string
	dsnEnv     string
	table      string // validated identifier, optionally schema-qualified (db.schema.table)
	columns    []string
	watermark  string
	wType      string // int64 | timestamp
	keyColumns []string
	batch      int
	orderBy    string
}

// NewSqlPollingConnector configures the connector. table may be
// "table" or "schema.table" (validated). watermarkType is int64 or timestamp.
func NewSqlPollingConnector(name, driver, dsnEnv, table, watermarkColumn, watermarkType string, keyColumns []string, batch int) (*SqlPollingConnector, error) {
	if !ValidName(name) {
		return nil, fmt.Errorf("integration: invalid connector name %q", name)
	}
	driver = normalizeSQLDriver(driver)
	switch driver {
	case "pgx", "postgres", "postgresql", "mysql", "sqlserver", "mssql", "oracle", "sqlite":
	default:
		return nil, fmt.Errorf("integration: unsupported driver %q (postgres|mysql|sqlserver|oracle|sqlite)", driver)
	}
	if dsnEnv == "" {
		return nil, errors.New("integration: dsn_env required (env var holding the DSN)")
	}
	if err := validateSQLIdent(table); err != nil {
		return nil, err
	}
	if err := validateSQLIdent(watermarkColumn); err != nil {
		return nil, err
	}
	switch watermarkType {
	case "int64", "timestamp":
	default:
		return nil, fmt.Errorf("integration: watermark_type must be int64|timestamp (got %q)", watermarkType)
	}
	for _, k := range keyColumns {
		if err := validateSQLIdent(k); err != nil {
			return nil, err
		}
	}
	if batch <= 0 {
		batch = 500
	}
	if batch > 2000 {
		batch = 2000
	}
	return &SqlPollingConnector{
		name:       name,
		driver:     driver,
		dsnEnv:     dsnEnv,
		table:      table,
		watermark:  watermarkColumn,
		wType:      watermarkType,
		keyColumns: keyColumns,
		batch:      batch,
		orderBy:    watermarkColumn,
	}, nil
}

func normalizeSQLDriver(d string) string {
	switch strings.ToLower(strings.TrimSpace(d)) {
	case "postgres", "postgresql", "pgx":
		return "pgx"
	case "mssql", "sql_server":
		return "sqlserver"
	case "ora", "oci":
		return "oracle"
	default:
		return strings.ToLower(strings.TrimSpace(d))
	}
}

func (s *SqlPollingConnector) Name() string { return s.name }

func (s *SqlPollingConnector) Close() error { return nil }

// FetchBatch reads the next watermark window.
func (s *SqlPollingConnector) FetchBatch(ctx context.Context, position string, max int) ([]Record, error) {
	if max <= 0 {
		max = s.batch
	}
	dsn := os.Getenv(s.dsnEnv)
	if dsn == "" {
		return nil, fmt.Errorf("integration: env %s is empty (DSN)", s.dsnEnv)
	}
	db, err := sql.Open(s.driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("integration: sql open (%s): %w", s.driver, err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(2)

	cols := "*"
	if len(s.columns) > 0 {
		cols = strings.Join(s.columns, ",")
	}
	var rows *sql.Rows
	switch s.wType {
	case "int64":
		var last int64
		if position != "" {
			v, err := json.Number(position).Int64()
			if err != nil {
				return nil, fmt.Errorf("integration: bad int64 watermark %q: %w", position, err)
			}
			last = v
		}
		q := fmt.Sprintf("SELECT %s FROM %s WHERE %s > ? ORDER BY %s ASC LIMIT %d", cols, s.table, s.watermark, s.orderBy, max)
		rows, err = db.QueryContext(ctx, q, last)
	default: // timestamp
		last := position
		if position != "" {
			if _, err := time.Parse(time.RFC3339Nano, position); err != nil {
				return nil, fmt.Errorf("integration: bad timestamp watermark %q: %w", position, err)
			}
			// Pass the position string as-is (lexical/ISO8601 comparison works
			// against text-formatted watermarks and typed timestamps alike).
		}
		q := fmt.Sprintf("SELECT %s FROM %s WHERE %s > ? ORDER BY %s ASC LIMIT %d", cols, s.table, s.watermark, s.orderBy, max)
		rows, err = db.QueryContext(ctx, q, last)
	}
	if err != nil {
		return nil, fmt.Errorf("integration: sql query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	names, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var out []Record
	for rows.Next() {
		vals := make([]any, len(names))
		ptrs := make([]any, len(vals))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("integration: sql scan: %w", err)
		}
		m := make(map[string]any, len(names))
		for i, n := range names {
			m[n] = normalizeSQLValue(vals[i])
		}
		r := Record{Value: mustJSON(m)}
		r.Position = watermarkString(m[s.watermark], s.wType)
		r.Headers = map[string]string{"sql_table": s.table, "watermark": s.watermark}
		r.Key = keyColumnsJSON(m, s.keyColumns)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func normalizeSQLValue(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case []byte:
		return string(x)
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano)
	case bool, string, int64, float64:
		return x
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case float32:
		return float64(x)
	default:
		return fmt.Sprint(x)
	}
}

func watermarkString(v any, wType string) string {
	if v == nil {
		return ""
	}
	switch wType {
	case "int64":
		switch x := v.(type) {
		case int64:
			return fmt.Sprint(x)
		case float64:
			return fmt.Sprint(int64(x))
		case string:
			return x
		}
		return fmt.Sprint(v)
	default: // timestamp
		switch x := v.(type) {
		case string:
			if _, err := time.Parse(time.RFC3339Nano, x); err == nil {
				return x
			}
			// try common driver layouts
			for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02 15:04:05.999999999", "2006-01-02T15:04:05Z07:00", "2006-01-02 15:04:05Z07:00"} {
				if t, err := time.Parse(layout, x); err == nil {
					return t.UTC().Format(time.RFC3339Nano)
				}
			}
			return x
		case time.Time:
			return x.UTC().Format(time.RFC3339Nano)
		}
		return fmt.Sprint(v)
	}
}

func keyColumnsJSON(m map[string]any, keyColumns []string) []byte {
	if len(keyColumns) == 0 {
		return []byte(fmt.Sprint(m["id"])) // best effort; may be nil
	}
	k := map[string]any{}
	for _, c := range keyColumns {
		k[c] = m[c]
	}
	b, err := json.Marshal(k)
	if err != nil {
		return nil
	}
	return b
}


func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		b = []byte(`{}`)
	}
	return b
}
