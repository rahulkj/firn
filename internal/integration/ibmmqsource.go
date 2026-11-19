package integration

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// IBMSinkSource browses the OLDEST message on an IBM MQ queue (GET is
// non-destructive), produces it to Firn, and then consumes it (DELETE).
// Single-consumer semantics: multiple connector instances would double-fetch.
// Position is informational (the queue owns state). At-least-once.
type IBMSinkSource struct {
	name     string
	url      string
	qmgr     string
	queue    string
	user     string
	pass     string
	caFile   string
	client   *http.Client
}

// NewIBMSinkSource creates the connector. url is the mqweb base
// (e.g. https://mq.example.com:9443/ibmmq/rest/v3). caEnv names an env var
// holding a PEM CA path; empty means system roots. passEnv names the env var
// holding the mqweb password.
func NewIBMSinkSource(name, url, qmgr, queue, user, passEnv, caEnv string) (*IBMSinkSource, error) {
	if !ValidName(name) {
		return nil, fmt.Errorf("integration: invalid connector name %q", name)
	}
	if url == "" || qmgr == "" || queue == "" || user == "" || passEnv == "" {
		return nil, errors.New("integration: ibmmq source requires url, qmgr, queue, user, pass_env")
	}
	pass := os.Getenv(passEnv)
	if pass == "" {
		return nil, fmt.Errorf("integration: env %s is empty (MQ password)", passEnv)
	}
	var pool *x509.CertPool
	if caEnv != "" {
		caFile := os.Getenv(caEnv)
		if caFile == "" {
			return nil, fmt.Errorf("integration: env %s is empty (MQ CA path)", caEnv)
		}
		b, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("integration: read ca %q: %w", caFile, err)
		}
		p, err := x509.SystemCertPool()
		if err != nil {
			p = x509.NewCertPool()
		}
		if !p.AppendCertsFromPEM(b) {
			return nil, fmt.Errorf("integration: ca %q: no certificates in PEM", caFile)
		}
		pool = p
	}
	return &IBMSinkSource{
		name:   name,
		url:    strings.TrimRight(url, "/"),
		qmgr:   qmgr,
		queue:  queue,
		user:   user,
		pass:   pass,
		caFile: caEnv,
		client: &http.Client{
			Timeout: 15 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
			},
		},
	}, nil
}

func (i *IBMSinkSource) Name() string { return i.name }

func (i *IBMSinkSource) Close() error { return nil }

// FetchBatch browses the queue head (non-destructive). At most ONE message
// per call: the head is only removed by Ack, so a multi-fetch would return
// the same message repeatedly. Position is the message id (informational).
func (i *IBMSinkSource) FetchBatch(ctx context.Context, position string, max int) ([]Record, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/messaging/qmgr/%s/queue/%s/message", i.url, i.qmgr, i.queue), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(i.user+":"+i.pass)))
	req.Header.Set("Accept", "text/plain, application/octet-stream")
	req.Header.Set("ibm-mq-rest-csrf-token", "firn")
	resp, err := i.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("integration: ibmmq get: %w", err)
	}
	if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotFound {
		_ = resp.Body.Close()
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != 201 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
		return nil, fmt.Errorf("integration: ibmmq get: HTTP %d: %.200s", resp.StatusCode, body)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	_ = resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("integration: ibmmq read body: %w", err)
	}
	r := Record{Value: body}
	r.Headers = map[string]string{"ibmmq_queue": i.queue, "ibmmq_qmgr": i.qmgr}
	pos := resp.Header.Get("ibm-mq-md-messageId")
	if pos == "" {
		pos = time.Now().UTC().Format(time.RFC3339Nano)
	}
	r.Headers["ibmmq_message_id"] = pos
	r.Position = pos
	return []Record{r}, nil
}

// Ack consumes the messages fetched in the last FetchBatch (DELETE oldest),
// preserving order. Called by the runner after Firn accepts the batch.
func (i *IBMSinkSource) Ack(ctx context.Context, records []Record) error {
	for range records {
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, fmt.Sprintf("%s/messaging/qmgr/%s/queue/%s/message", i.url, i.qmgr, i.queue), nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(i.user+":"+i.pass)))
		req.Header.Set("ibm-mq-rest-csrf-token", "firn")
		resp, err := i.client.Do(req)
		if err != nil {
			return fmt.Errorf("integration: ibmmq delete: %w", err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode >= 300 && resp.StatusCode != 404 {
			return fmt.Errorf("integration: ibmmq delete: HTTP %d (message may not have been consumed; will redeliver)", resp.StatusCode)
		}
	}
	return nil
}
