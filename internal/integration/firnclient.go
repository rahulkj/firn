package integration

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"github.com/rahulkj/firn/sdk/firngo"
)

// FirnConn describes how an integration process reaches Firn.
type FirnConn struct {
	Addr     string // host:port of firnd's client gRPC listener
	APIKey   string // bearer key; empty = dev broker without keys
	CAFile   string // PEM CA bundle to verify the broker cert ("" = system roots)
	Insecure bool   // plaintext (dev only)
}

// DialFirn builds a client from the connection description.
func DialFirn(ctx context.Context, fc FirnConn) (*firngo.Client, error) {
	if fc.Addr == "" {
		return nil, errors.New("firn address required")
	}
	if fc.Insecure {
		return firngo.DialInsecure(ctx, fc.Addr)
	}
	if fc.APIKey == "" {
		return nil, errors.New("firn api key required (set the configured env var), or set insecure: true for dev")
	}
	if fc.CAFile == "" {
		return firngo.DialProduction(ctx, fc.Addr, fc.APIKey, nil)
	}
	pemBytes, err := os.ReadFile(fc.CAFile)
	if err != nil {
		return nil, fmt.Errorf("firn CA %q: %w", fc.CAFile, err)
	}
	pool, err := x509.SystemCertPool()
	if err != nil {
		pool = x509.NewCertPool()
	}
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("firn CA %q: no certificates found in PEM", fc.CAFile)
	}
	cfg := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	return firngo.DialWithTLS(ctx, fc.Addr, fc.APIKey, cfg)
}
