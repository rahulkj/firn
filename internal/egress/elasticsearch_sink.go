package egress

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// ElasticsearchSink bulk-indexes each record into one index via the _bulk API.
// Record value must be a JSON object; record key (if set) becomes the document _id.
// Optional ingest pipeline applies server-side processing.
type ElasticsearchSink struct {
	url      string
	index    string
	pipeline string
	token    string // empty = no auth
	hc       *http.Client
}

func newElasticsearchSink(r Route) (Sink, error) {
	s := &ElasticsearchSink{
		url:      strings.TrimRight(r.Sink.URL, "/"),
		index:    r.Sink.Index,
		pipeline: r.Sink.Pipeline,
		hc:       &http.Client{Timeout: 30 * time.Second},
	}
	if r.Sink.AuthEnv != "" {
		s.token = os.Getenv(r.Sink.AuthEnv)
		if s.token == "" {
			return nil, fmt.Errorf("egress: route %q: env %s is empty (elasticsearch bearer token)", r.Name, r.Sink.AuthEnv)
		}
	}
	return s, nil
}

type bulkResponse struct {
	Errors bool `json:"errors"`
	Items  []struct {
		Index struct {
			Status int             `json:"status"`
			Error  json.RawMessage `json:"error,omitempty"`
		} `json:"index"`
	} `json:"items"`
}

func (s *ElasticsearchSink) Write(ctx context.Context, route string, batch []Delivery) error {
	if len(batch) == 0 {
		return nil
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, d := range batch {
		action := map[string]map[string]string{}
		se := map[string]string{"_index": s.index}
		if s.pipeline != "" {
			se["pipeline"] = s.pipeline
		}
		if d.Key != nil {
			se["_id"] = string(d.Key)
		}
		action["index"] = se
		if err := enc.Encode(action); err != nil {
			return err
		}
		if err := enc.Encode(json.RawMessage(d.Value)); err != nil {
			return fmt.Errorf("egress: elasticsearch: record value is not valid JSON: %w", err)
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url+"/_bulk", &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	if s.token != "" {
		req.Header.Set("Authorization", "Bearer "+s.token)
	}
	resp, err := s.hc.Do(req)
	if err != nil {
		return fmt.Errorf("egress: elasticsearch bulk: %w (retryable)", err)
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("egress: elasticsearch bulk: HTTP %d: %.200s (retryable)", resp.StatusCode, body)
	}
	var br bulkResponse
	if err := json.Unmarshal(body, &br); err != nil {
		return fmt.Errorf("egress: elasticsearch bulk decode: %w (retryable)", err)
	}
	if br.Errors {
		failed := 0
		firstErr := ""
		for _, item := range br.Items {
			if item.Index.Status >= 300 {
				failed++
				if firstErr == "" {
					firstErr = string(item.Index.Error)
				}
			}
		}
		return fmt.Errorf("egress: elasticsearch bulk had %d failed items (first: %s) (retryable)", failed, firstErr)
	}
	return nil
}

func (s *ElasticsearchSink) IsTerminal(err error) bool { return false }

func (s *ElasticsearchSink) Close() error { return nil }
