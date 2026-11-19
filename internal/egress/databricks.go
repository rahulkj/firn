package egress

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// DatabricksSink uploads each rendered batch to a Unity Catalog Volume via
// the Databricks Files API (PUT /api/2.0/fs/files/<path>?overwrite=true).
// Write strategy: PUT the .jsonl file, then PUT the .manifest.json — both
// with overwrite=true, which makes retries idempotent (content is a pure
// function of the offset range). There is no atomic rename over the Files
// API, so an interrupted upload may leave a truncated .jsonl; the manifest
// is written after it, so consumers should gate on the manifest's presence.
// Documented in docs/integrations.md.

type databricksSink struct {
	workspace string
	volPath   string
	token     string
	hc        *http.Client
}

func newDatabricksSink(r Route) (Sink, error) {
	return &databricksSink{
		workspace: strings.TrimRight(r.Sink.WorkspaceURL, "/"),
		volPath:   strings.TrimRight(r.Sink.VolumePath, "/"),
		token:     os.Getenv(r.Sink.TokenEnv),
		hc:        &http.Client{Timeout: 60 * time.Second},
	}, nil
}

func (d *databricksSink) Write(ctx context.Context, route string, batch []Delivery) error {
	parts, err := renderPartitions(route, batch)
	if err != nil {
		return err
	}
	for _, p := range parts {
		for _, file := range []struct{ name string; data []byte }{
			{p.filename, p.jsonl},
			{p.filename + ".manifest.json", p.manifest},
		} {
			url := fmt.Sprintf("%s/api/2.0/fs/files%s/%s/%s?overwrite=true", d.workspace, d.volPath, route, file.name)
			req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, strings.NewReader(string(file.data)))
			if err != nil {
				return err
			}
			req.Header.Set("Authorization", "Bearer "+d.token)
			req.Header.Set("Content-Type", "application/octet-stream")
			resp, err := d.hc.Do(req)
			if err != nil {
				return fmt.Errorf("databricks sink PUT %s: %w", file.name, err)
			}
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
			if resp.StatusCode >= 300 {
				if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
					return &TerminalError{Err: fmt.Errorf("databricks sink auth failed on %s: HTTP %d", file.name, resp.StatusCode)}
				}
				return fmt.Errorf("databricks sink PUT %s: HTTP %d (retryable)", file.name, resp.StatusCode)
			}
		}
	}
	return nil
}

func (d *databricksSink) IsTerminal(err error) bool {
	var te *TerminalError
	return errorsAs(err, &te)
}

func (d *databricksSink) Close() error { return nil }

func errorsAs(err error, target **TerminalError) bool {
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
