package egress

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"testing"
	"time"
)

// Live Elasticsearch sink test. Requires ES_TEST_URL (e.g. http://127.0.0.1:9200).
// Creates and deletes a test index firn_sink_test.
func TestElasticsearchSinkLive(t *testing.T) {
	url := envOrSkip(t, "ES_TEST_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Setup: create a fresh index via the REST API.
	delReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, url+"/firn_sink_test", nil)
	if err == nil {
		_, _ = http.DefaultClient.Do(delReq)
	}
	mkReq, err := http.NewRequestWithContext(ctx, http.MethodPut, url+"/firn_sink_test", bytes.NewReader([]byte(`{}`)))
	if err != nil {
		t.Fatal(err)
	}
	mkReq.Header.Set("Content-Type", "application/json")
	mkResp, err := http.DefaultClient.Do(mkReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = mkResp.Body.Close()

	r := Route{Name: "es-out"}
	r.Sink = SinkConfig{Type: "elasticsearch", URL: url, Index: "firn_sink_test", Auth: "none"}
	sink, err := NewSink(r)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sink.Close() }()

	batch := []Delivery{
		{Value: []byte(`{"id":"a","note":"alpha"}`), Offset: 0, Partition: 0, Key: []byte("a")},
		{Value: []byte(`{"id":"b","note":"beta"}`), Offset: 1, Partition: 0, Key: []byte("b")},
	}
	if err := sink.Write(ctx, "es-out", batch); err != nil {
		t.Fatal(err)
	}

	// Verify documents are searchable (refresh=true via _refresh).
	_, _ = http.Post(url+"/firn_sink_test/_refresh", "application/json", nil)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url+"/firn_sink_test/_count", nil)
	if err != nil {
		t.Fatal(err)
	}
	countResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(io.LimitReader(countResp.Body, 4096))
	_ = countResp.Body.Close()
	want := `"count":2`
	if !bytes.Contains(body, []byte(want)) {
		t.Fatalf("ES count response = %s, want contains %s", body, want)
	}
	// Cleanup.
	dreq, err := http.NewRequestWithContext(ctx, http.MethodDelete, url+"/firn_sink_test", nil)
	if err == nil {
		_, _ = http.DefaultClient.Do(dreq)
	}
}

// Live ClickHouse sink test. Requires CH_TEST_URL (e.g. http://127.0.0.1:8123).
// Creates and drops a test table.
func TestClickHouseSinkLive(t *testing.T) {
	url := envOrSkip(t, "CH_TEST_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pass := os.Getenv("CH_TEST_PASSWORD")
	createQ := "CREATE DATABASE IF NOT EXISTS firn_sink_test"
	chDo(t, url, pass, createQ)
	chDo(t, url, pass, "DROP TABLE IF EXISTS firn_sink_test.events")
	chDo(t, url, pass, "CREATE TABLE firn_sink_test.events (id String, note String, qty Int64) ENGINE = Memory")

	r := Route{Name: "ch-out"}
	r.Sink = SinkConfig{Type: "clickhouse", URL: url, Index: "events", Database: "firn_sink_test", CHUserEnv: "CH_TEST_USER", CHPasswordEnv: "CH_TEST_PW"}
	t.Setenv("CH_TEST_USER", "default")
	if err := func() error {
		sink, err := NewSink(r)
		if err != nil {
			return err
		}
		batch := []Delivery{
			{Value: []byte(`{"id":"a","note":"alpha","qty":1}`), Offset: 0, Partition: 0},
			{Value: []byte(`{"id":"b","note":"beta","qty":2}`), Offset: 1, Partition: 0},
		}
		return sink.Write(ctx, "ch-out", batch)
	}(); err != nil {
		t.Fatal(err)
	}
	count := chDo(t, url, pass, "SELECT count() FROM firn_sink_test.events")
	if string(count) != "2\n" {
		t.Fatalf("clickhouse count = %q, want 2\\n", count)
	}
	chDo(t, url, pass, "DROP DATABASE firn_sink_test")
}

func chDo(t *testing.T, url, pass, query string) []byte {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url+"/?user=default&password="+pass, bytes.NewReader([]byte(query)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "text/plain")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("clickhouse query %q: HTTP %d: %s", query, resp.StatusCode, body)
	}
	return body
}
