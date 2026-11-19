package egress

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// IBMSink publishes record values to an IBM MQ queue through the mqweb
// REST API (IBM MQ 9.1+). No IBM client libraries required. At-least-once
// per route policy. Requires HTTPS; the queue manager must expose the
// mqweb service with messaging resources enabled and a user in mqm
// (or a user authorized for the queue).
type IBMSink struct {
	url      string // base, e.g. https://mq.example.com:9443/ibmmq/rest/v3
	qmgr     string
	queue    string
	user     string
	pass     string
	caFile   string
	client   *http.Client
}

func newIBMSink(r Route) (Sink, error) {
	s := &IBMSink{
		url:    strings.TrimRight(r.Sink.MQWebURL, "/"),
		qmgr:   r.Sink.MQWebQueueMgr,
		queue:  r.Sink.MQWebQueue,
		user:   r.Sink.MQWebUser,
		pass:   os.Getenv(r.Sink.MQWebPassEnv),
		caFile: r.Sink.MQWebCAEnv,
	}
	if s.caFile != "" {
		s.caFile = os.Getenv(s.caFile)
	}
	pool, err := loadCAPool(s.caFile)
	if err != nil {
		return nil, err
	}
	s.client = &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
		},
	}
	return s, nil
}

func loadCAPool(caFile string) (*x509.CertPool, error) {
	if caFile == "" {
		return nil, nil // nil pool = system roots
	}
	b, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("ibmmq: read ca %q: %w", caFile, err)
	}
	pool, err := x509.SystemCertPool()
	if err != nil {
		pool = x509.NewCertPool()
	}
	if !pool.AppendCertsFromPEM(b) {
		return nil, fmt.Errorf("ibmmq: ca %q: no certificates in PEM", caFile)
	}
	return pool, nil
}

func (s *IBMSink) Write(ctx context.Context, route string, batch []Delivery) error {
	for _, d := range batch {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/messaging/qmgr/%s/queue/%s/message", s.url, s.qmgr, s.queue), strings.NewReader(string(d.Value)))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(s.user+":"+s.pass)))
		req.Header.Set("Content-Type", "text/plain;charset=utf-8")
		req.Header.Set("ibm-mq-rest-csrf-token", "firn")
		req.Header.Set("ibm-mq-md-persistence", "persistent")
		if d.Key != nil {
			req.Header.Set("ibm-mq-md-correlationId", "ID:"+keyToHex(d.Key))
		}
		resp, err := s.client.Do(req)
		if err != nil {
			return fmt.Errorf("egress: ibmmq post: %w (retryable)", err)
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
		switch {
		case resp.StatusCode >= 200 && resp.StatusCode < 300:
		case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
			return fmt.Errorf("egress: ibmmq post: HTTP %d (retryable)", resp.StatusCode)
		default:
			return &TerminalError{Err: fmt.Errorf("egress: ibmmq post: HTTP %d: %.200s", resp.StatusCode, body)}
		}
	}
	return nil
}

func (s *IBMSink) IsTerminal(err error) bool {
	var te *TerminalError
	return errorsAsAs(err, &te)
}

func (s *IBMSink) Close() error { return nil }

func keyToHex(key []byte) string {
	const hexdigits = "0123456789abcdef"
	out := make([]byte, 0, len(key)*2)
	for _, b := range key {
		out = append(out, hexdigits[b>>4], hexdigits[b&0xf])
	}
	return string(out)
}
