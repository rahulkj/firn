package egress

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// ClickHouseSink bulk-inserts JSONEachRow via ClickHouse's HTTP interface:
//   POST <url>/?database=<db>&query=INSERT INTO <table> FORMAT JSONEachRow
// The record value must be a JSON object whose keys match the target table's
// columns (unknown columns are rejected server-side; missing columns are
// filled by server defaults).
type ClickHouseSink struct {
	url      string
	database string
	table    string
	user     string
	pass     string
	hc       *http.Client
}

func newClickHouseSink(r Route) (Sink, error) {
	db := r.Sink.Database
	if db == "" {
		db = "default"
	}
	user := "default"
	if r.Sink.CHUserEnv != "" {
		user = os.Getenv(r.Sink.CHUserEnv)
		if user == "" {
			return nil, fmt.Errorf("egress: route %q: env %s is empty (clickhouse user)", r.Name, r.Sink.CHUserEnv)
		}
	}
	pass := ""
	if r.Sink.CHPasswordEnv != "" {
		pass = os.Getenv(r.Sink.CHPasswordEnv)
		if pass == "" {
			return nil, fmt.Errorf("egress: route %q: env %s is empty (clickhouse password)", r.Name, r.Sink.CHPasswordEnv)
		}
	}
	return &ClickHouseSink{
		url:      strings.TrimRight(r.Sink.URL, "/"),
		database: db,
		table:    r.Sink.Index, // index field doubles as the table name
		user:     user,
		pass:     pass,
		hc:       &http.Client{Timeout: 30 * time.Second},
	}, nil
}

func (s *ClickHouseSink) Write(ctx context.Context, route string, batch []Delivery) error {
	if len(batch) == 0 {
		return nil
	}
	var buf bytes.Buffer
	for _, d := range batch {
		if !json.Valid(d.Value) {
			return fmt.Errorf("egress: clickhouse: record value is not valid JSON")
		}
		buf.Write(d.Value)
		buf.WriteByte('\n')
	}
	q := url.Values{}
	q.Set("database", s.database)
	q.Set("query", fmt.Sprintf("INSERT INTO %s FORMAT JSONEachRow", s.table))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url+"?"+q.Encode(), &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if s.user != "" || s.pass != "" {
		req.Header.Set("X-ClickHouse-User", s.user)
		req.Header.Set("X-ClickHouse-Key", s.pass)
	}
	resp, err := s.hc.Do(req)
	if err != nil {
		return fmt.Errorf("egress: clickhouse insert: %w (retryable)", err)
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("egress: clickhouse insert: HTTP %d: %.200s", resp.StatusCode, body)
	}
	return nil
}

func (s *ClickHouseSink) IsTerminal(err error) bool { return false }

func (s *ClickHouseSink) Close() error { return nil }
