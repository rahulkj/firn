package egress

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	_ "modernc.org/sqlite"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/microsoft/go-mssqldb"
	_ "github.com/sijms/go-ora/v2"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// SqlSink writes JSON row records into a relational table, one transaction
// per batch, upserting on the configured key columns by default. Dialect
// coverage: Postgres/SQLite/Cockroach (ON CONFLICT), MySQL/MariaDB
// (ON DUPLICATE KEY UPDATE), SQL Server / Oracle (MERGE). Set upsert=false
// for append-only INSERT.
//
// Record shape: each delivery value must be a JSON object; one column per
// object key. Columns missing in some rows are written as NULL. The key
// columns must have a uniqueness constraint (the upsert depends on it).
type SqlSink struct {
	driver    string
	dsnEnv    string
	table     string
	keyCols   []string
	upsert    bool
	colFilter []string // optional explicit column allow-list
}

func newSqlSink(r Route) (Sink, error) {
	driver := normalizeSqlDriver(r.Sink.Driver)
	switch driver {
	case "pgx", "mysql", "sqlserver", "oracle", "sqlite":
	default:
		return nil, fmt.Errorf("egress: route %q: sql sink driver must be pgx|mysql|sqlserver|oracle|sqlite (got %q)", r.Name, r.Sink.Driver)
	}
	if r.Sink.DSNEnv == "" {
		return nil, fmt.Errorf("egress: route %q: sql sink requires dsn_env", r.Name)
	}
	if os.Getenv(r.Sink.DSNEnv) == "" {
		return nil, fmt.Errorf("egress: route %q: env %s is empty", r.Name, r.Sink.DSNEnv)
	}
	if err := validateSQLIdent(r.Sink.Table); err != nil {
		return nil, fmt.Errorf("egress: route %q: sql sink table: %w", r.Name, err)
	}
	for _, k := range r.Sink.KeyColumns {
		if err := validateSQLIdent(k); err != nil {
			return nil, fmt.Errorf("egress: route %q: sql sink key column: %w", r.Name, err)
		}
	}
	upsert := !r.Sink.AppendOnly
	if upsert && len(r.Sink.KeyColumns) == 0 {
		return nil, fmt.Errorf("egress: route %q: sql sink upsert requires key_columns (or set append_only: true)", r.Name)
	}
	for _, k := range r.Sink.KeyColumns {
		if err := validateSQLIdent(k); err != nil {
			return nil, fmt.Errorf("egress: route %q: sql sink key column: %w", r.Name, err)
		}
	}
	return &SqlSink{driver: driver, dsnEnv: r.Sink.DSNEnv, table: r.Sink.Table, keyCols: r.Sink.KeyColumns, upsert: upsert, colFilter: r.Sink.Columns}, nil
}

func validateSQLIdent(s string) error {
	if s == "" || len(s) > 128 {
		return errors.New("identifier empty or too long")
	}
	for _, part := range strings.Split(s, ".") {
		if part == "" {
			return errors.New("empty identifier segment")
		}
		for _, r := range part {
			ok := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_'
			if !ok {
				return fmt.Errorf("invalid character %q", r)
			}
		}
	}
	return nil
}

func normalizeSqlDriver(d string) string {
	switch strings.ToLower(strings.TrimSpace(d)) {
	case "postgres", "postgresql", "pgx":
		return "pgx"
	case "mariadb":
		return "mysql"
	case "mssql", "sql_server":
		return "sqlserver"
	case "ora", "oci":
		return "oracle"
	default:
		return strings.ToLower(strings.TrimSpace(d))
	}
}

func (s *SqlSink) Write(ctx context.Context, route string, batch []Delivery) error {
	if len(batch) == 0 {
		return nil
	}
	dsn := os.Getenv(s.dsnEnv)
	db, err := sql.Open(s.driver, dsn)
	if err != nil {
		return fmt.Errorf("sql sink open (%s): %w", s.driver, err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(2)

	rows := make([]map[string]any, 0, len(batch))
	for _, d := range batch {
		var m map[string]any
		if err := json.Unmarshal(d.Value, &m); err != nil {
			return &TerminalError{Err: fmt.Errorf("sql sink: record value is not a JSON object: %w", err)}
		}
		if len(s.colFilter) > 0 {
			filtered := map[string]any{}
			for _, c := range s.colFilter {
				if v, ok := m[c]; ok {
					filtered[c] = v
				}
			}
			m = filtered
		}
		if len(m) == 0 {
			return &TerminalError{Err: fmt.Errorf("sql sink: record has no columns after filter")}
		}
		rows = append(rows, m)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sql sink begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, row := range rows {
		if err := s.writeRow(ctx, tx, route, row); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sql sink commit: %w", err)
	}
	return nil
}

func (s *SqlSink) writeRow(ctx context.Context, tx *sql.Tx, route string, row map[string]any) error {
	cols := sortedKeys3(row)
	placeholders := sqlPlaceholderList(s.driver, len(cols))
	insertCols := strings.Join(cols, ",")
	switch s.driver {
	case "pgx", "sqlite":
		setClause := buildSetClause(cols, s.keyCols)
		conflict := strings.Join(s.keyCols, ",")
		q := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s", s.table, insertCols, placeholders, conflict, setClause)
		if len(s.keyCols) == 0 || !s.upsert {
			q = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", s.table, insertCols, placeholders)
		}
		_, err := tx.ExecContext(ctx, q, rowArgs(cols, row)...)
		return err
	case "mysql":
		setClause := buildMySQLSetClause(cols, s.keyCols)
		q := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s", s.table, insertCols, placeholders, setClause)
		if len(s.keyCols) == 0 || !s.upsert {
			q = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", s.table, insertCols, placeholders)
		}
		_, err := tx.ExecContext(ctx, q, rowArgs(cols, row)...)
		return err
	default: // sqlserver, oracle — MERGE
		src := buildValuesSubquery(s.driver, cols)
		match := buildMergeMatchIndexed(cols, s.keyCols)
		setClause := buildSetClauseIndexed(cols, s.keyCols)
		args := rowArgs(cols, row)
		var valuesList string
		if s.driver == "sqlserver" {
			valuesList = sqlPlaceholderListAliased("src", len(cols))
		} else {
			// Oracle forbids source-column references in INSERT VALUES;
			// re-bind the same values with fresh positional placeholders.
			valuesList = lowerPlaceholders(sqlPlaceholderList(s.driver, len(cols)), len(cols))
			args = append(args, rowArgs(cols, row)...)
		}
		q := fmt.Sprintf(`MERGE INTO %s dst USING %s ON %s WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)`,
			s.table, src, match, setClause, insertCols, valuesList)
		if len(s.keyCols) == 0 || !s.upsert {
			q = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", s.table, insertCols, placeholders)
		}
		_, err := tx.ExecContext(ctx, q, args...)
		return err
	}
}

func (s *SqlSink) IsTerminal(err error) bool {
	var te *TerminalError
	return errorsAsAs(err, &te)
}
func (s *SqlSink) Close() error { return nil }

func errorsAsAs(err error, target **TerminalError) bool {
	for err != nil {
		if e, ok := err.(*TerminalError); ok {
			*target = e
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}


// sqlPlaceholders returns n positional placeholders for the given driver.
// postgres(pgx) uses $N, sqlite and mysql use ?, sqlserver uses @pN, oracle uses :N.
func sqlPlaceholders(driver string, n int) []string {
	out := make([]string, 0, n)
	for i := 1; i <= n; i++ {
		switch driver {
		case "pgx":
			out = append(out, fmt.Sprintf("$%d", i))
		case "sqlserver":
			out = append(out, fmt.Sprintf("@p%d", i))
		case "oracle":
			out = append(out, fmt.Sprintf(":%d", i))
		default: // mysql, sqlite
			out = append(out, "?")
		}
	}
	return out
}

func sqlPlaceholderList(driver string, n int) string {
	return strings.Join(sqlPlaceholders(driver, n), ",")
}

func sortedKeys3(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

func sqlPlaceholderListAliased(alias string, n int) string {
	parts := make([]string, n)
	for i := range parts {
		parts[i] = fmt.Sprintf("%s.c%d", alias, i+1)
	}
	return strings.Join(parts, ",")
}

func buildSetClause(cols, keyCols []string) string {
	keys := map[string]bool{}
	for _, k := range keyCols {
		keys[k] = true
	}
	sets := []string{}
	for _, c := range cols {
		if keys[c] {
			continue
		}
		sets = append(sets, fmt.Sprintf("%s = EXCLUDED.%s", c, c))
	}
	if len(sets) == 0 {
		return "id = EXCLUDED.id" // degenerate; shouldn't happen with key-only rows
	}
	return strings.Join(sets, ",")
}

func buildSetClauseIndexed(cols, keyCols []string) string {
	keys := map[string]bool{}
	for _, k := range keyCols {
		keys[k] = true
	}
	sets := []string{}
	for i, c := range cols {
		if keys[c] {
			continue
		}
		sets = append(sets, fmt.Sprintf("dst.%s = src.c%d", c, i+1))
	}
	if len(sets) == 0 {
		// key-only rows: update nothing meaningful
		for i, c := range cols {
			if keys[c] {
				sets = append(sets, fmt.Sprintf("dst.%s = src.c%d", c, i+1))
			}
		}
	}
	return strings.Join(sets, ",")
}

func buildMergeMatchIndexed(cols, keyCols []string) string {
	idx := map[string]int{}
	for i, c := range cols {
		idx[c] = i + 1
	}
	parts := make([]string, 0, len(keyCols))
	for _, k := range keyCols {
		parts = append(parts, fmt.Sprintf("dst.%s = src.c%d", k, idx[k]))
	}
	// Oracle's MERGE grammar requires the ON condition to be parenthesized;
	// SQL Server accepts the same shape, so always parenthesize.
	return "(" + strings.Join(parts, " AND ") + ")"
}

func buildValuesSubquery(driver string, cols []string) string {
	ph := sqlPlaceholders(driver, len(cols))
	parts := make([]string, len(cols))
	for i := range cols {
		parts[i] = fmt.Sprintf("%s AS c%d", ph[i], i+1)
	}
	q := "(SELECT " + strings.Join(parts, ",")
	if driver == "oracle" {
		q += " FROM dual" // Oracle <23 requires FROM DUAL
	}
	return q + ") src"
}

func buildMySQLSetClause(cols, keyCols []string) string {
	keys := map[string]bool{}
	for _, k := range keyCols {
		keys[k] = true
	}
	sets := []string{}
	for _, c := range cols {
		if keys[c] {
			continue
		}
		sets = append(sets, fmt.Sprintf("%s = VALUES(%s)", c, c))
	}
	if len(sets) == 0 {
		return keyCols[0] + " = VALUES(" + keyCols[0] + ")"
	}
	return strings.Join(sets, ",")
}

func rowArgs(cols []string, row map[string]any) []any {
	out := make([]any, 0, len(cols))
	for _, c := range cols {
		out = append(out, jsonToSQLValue(row[c]))
	}
	return out
}

func jsonToSQLValue(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case bool:
		return x
	case string:
		return x
	case float64:
		// Integral floats round-trip as int64 (common for ids).
		if x == float64(int64(x)) {
			return int64(x)
		}
		return x
	case float32:
		return float64(x)
	case map[string]any, []any:
		b, err := json.Marshal(x)
		if err != nil {
			return nil
		}
		return string(b)
	default:
		return fmt.Sprint(x)
	}
}



// lowerPlaceholders renumbers 1-based placeholders (":N") to start at len(cols)+1 (":len+1" onward).
func lowerPlaceholders(ph string, offset int) string {
	if ph == "" {
		return ""
	}
	// Split, renumber each :N or $N or @pN.
	parts := strings.Split(ph, ",")
	for i, p := range parts {
		p = strings.TrimSpace(p)
		var num int
		switch {
		case strings.HasPrefix(p, ":"):
			_, _ = fmt.Sscanf(p[1:], "%d", &num)
			num += offset
			parts[i] = fmt.Sprintf(":%d", num)
		case strings.HasPrefix(p, "@p"):
			_, _ = fmt.Sscanf(p[2:], "%d", &num)
			num += offset
			parts[i] = fmt.Sprintf("@p%d", num)
		case strings.HasPrefix(p, "$"):
			_, _ = fmt.Sscanf(p[1:], "%d", &num)
			num += offset
			parts[i] = fmt.Sprintf("$%d", num)
		}
	}
	return strings.Join(parts, ",")
}
