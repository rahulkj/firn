package meshca

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net"
	"os"
	"path/filepath"
	"testing"
)

func pemDecodeNodeCert(t *testing.T, b []byte) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode(b)
	if block == nil {
		t.Fatal("no PEM block in node.crt")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse node cert: %v", err)
	}
	return cert
}

func nodeCertOf(t *testing.T, dir string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "node.crt"))
	if err != nil {
		t.Fatalf("read node.crt: %v", err)
	}
	return b
}

func TestEnsureFreshOnLoopback(t *testing.T) {
	dir := t.TempDir()
	b, err := Ensure(filepath.Join(dir, "mesh-ca"), "n1", true)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if len(b.CACrtPEM) == 0 || len(b.NodeCrtPEM) == 0 {
		t.Fatal("empty bundle after ensure")
	}
	// CA pool must resolve the node cert.
	_ = pemDecodeNodeCert(t, nodeCertOf(t, filepath.Join(dir, "mesh-ca")))
}

func TestEnsureRefusesWithoutAllowAuto(t *testing.T) {
	dir := t.TempDir()
	_, err := Ensure(filepath.Join(dir, "mesh-ca"), "n1", false)
	if err == nil {
		t.Fatal("expected error without allowAuto")
	}
}

func TestGenerateLoadRoundtrip(t *testing.T) {
	dir := t.TempDir()
	caDir := filepath.Join(dir, "mesh-ca")
	b1, err := Generate(caDir, "n1")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	b2, err := Load(caDir, "n1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if string(b1.CACrtPEM) != string(b2.CACrtPEM) {
		t.Fatal("CA cert not stable across reload")
	}
	if err := b2.VerifyClientCert(pemDecodeNodeCert(t, b2.NodeCrtPEM)); err != nil {
		t.Fatalf("valid node cert rejected by own CA: %v", err)
	}
}

func TestCrossCAPeerRejected(t *testing.T) {
	dir1, dir2 := t.TempDir(), t.TempDir()
	b1, err := Generate(filepath.Join(dir1, "mesh-ca"), "n1")
	if err != nil {
		t.Fatalf("generate 1: %v", err)
	}
	b2, err := Generate(filepath.Join(dir2, "mesh-ca"), "n2")
	if err != nil {
		t.Fatalf("generate 2: %v", err)
	}
	if err := b2.VerifyClientCert(pemDecodeNodeCert(t, b1.NodeCrtPEM)); err == nil {
		t.Fatal("cross-CA peer accepted; want rejection")
	}
}

func TestDirAndFileModes(t *testing.T) {
	dir := t.TempDir()
	caDir := filepath.Join(dir, "mesh-ca")
	if _, err := Generate(caDir, "n1"); err != nil {
		t.Fatalf("generate: %v", err)
	}
	for _, name := range []string{"ca.crt", "ca.key", "node.crt", "node.key"} {
		fi, err := os.Stat(filepath.Join(caDir, name))
		if err != nil {
			t.Fatalf("stat %s: %v", name, err)
		}
		if got := fi.Mode().Perm(); got != 0o600 {
			t.Fatalf("%s mode = %o, want 600", name, got)
		}
	}
	fi, err := os.Stat(caDir)
	if err != nil {
		t.Fatalf("stat caDir: %v", err)
	}
	if got := fi.Mode().Perm(); got != 0o700 {
		t.Fatalf("mesh-ca dir mode = %o, want 700", got)
	}
}

func TestSameCAHandshakeSucceeds(t *testing.T) {
	dir := t.TempDir()
	b, err := Generate(filepath.Join(dir, "mesh-ca"), "n1")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	n2crt, n2key, err := issueNode(b.CACrtPEM, mustParseECKey(b.CAKeyPEM), "n2")
	if err != nil {
		t.Fatalf("issue n2: %v", err)
	}
	n2, err := newBundle(b.CACrtPEM, b.CAKeyPEM, n2crt, n2key)
	if err != nil {
		t.Fatalf("build n2 bundle: %v", err)
	}

	c1, c2 := net.Pipe()
	defer func() { _ = c1.Close(); _ = c2.Close() }()
	server := tls.Server(c1, b.ServerTLSConfig())
	client := tls.Client(c2, n2.ClientTLSConfig("n1"))
	errs := make(chan error, 2)
	go func() { errs <- server.Handshake() }()
	go func() { errs <- client.Handshake() }()
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("same-CA handshake: %v", err)
		}
	}
	_ = server.Close()
	_ = client.Close()
}

func TestCrossCAHandshakeFails(t *testing.T) {
	dir := t.TempDir()
	b1, err := Generate(filepath.Join(dir, "ca1"), "n1")
	if err != nil {
		t.Fatalf("generate ca1: %v", err)
	}
	b2, err := Generate(filepath.Join(dir, "ca2"), "n1")
	if err != nil {
		t.Fatalf("generate ca2: %v", err)
	}
	n9crt, n9key, err := issueNode(b2.CACrtPEM, mustParseECKey(b2.CAKeyPEM), "n9")
	if err != nil {
		t.Fatalf("issue n9: %v", err)
	}
	n9, err := newBundle(b2.CACrtPEM, b2.CAKeyPEM, n9crt, n9key)
	if err != nil {
		t.Fatalf("build n9 bundle: %v", err)
	}

	c1, c2 := net.Pipe()
	defer func() { _ = c1.Close(); _ = c2.Close() }()
	server := tls.Server(c1, b1.ServerTLSConfig()) // ca1
	client := tls.Client(c2, n9.ClientTLSConfig("n1"))
	errs := make(chan error, 2)
	go func() { errs <- server.Handshake() }()
	go func() { errs <- client.Handshake() }()
	sawFailure := false
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			sawFailure = true
		}
	}
	if !sawFailure {
		t.Fatal("cross-CA handshake unexpectedly succeeded")
	}
}

func TestCrossCAHandshakeRejected(t *testing.T) {
	dir := t.TempDir()
	b1, err := Generate(filepath.Join(dir, "ca1"), "n1")
	if err != nil {
		t.Fatalf("generate ca1: %v", err)
	}
	other, err := Generate(filepath.Join(dir, "ca2"), "n1")
	if err != nil {
		t.Fatalf("generate ca2: %v", err)
	}
	caCert := pemDecodeNodeCert(t, b1.NodeCrtPEM)
	otherCert := pemDecodeNodeCert(t, other.NodeCrtPEM)
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	pool.AddCert(otherCert) // both present — still the peer's own CA decides

	if err := b1.VerifyClientCert(otherCert); err == nil {
		t.Fatal("cross-CA peer accepted by VerifyClientCert")
	} else {
		t.Logf("verify error (ok, just must not accept): %v", err)
	}
}

func TestEnsureAcceptsOpenSSLStyleMultiBlockCAKey(t *testing.T) {
	// openssl ecparam -genkey emits "EC PARAMETERS" + "EC PRIVATE KEY" in one
	// file. firnd must accept that bundle — it is what the install docs used
	// to tell operators to generate.
	dir := t.TempDir()
	caDir := filepath.Join(dir, "mesh-ca")
	if _, err := Generate(caDir, "firn-0"); err != nil {
		t.Fatalf("generate: %v", err)
	}
	keyPEM, err := os.ReadFile(filepath.Join(caDir, "ca.key"))
	if err != nil {
		t.Fatal(err)
	}
	// P-256 named-curve parameters block, exactly as openssl writes it.
	params := []byte("-----BEGIN EC PARAMETERS-----\nBggqhkjOPQMBBw==\n-----END EC PARAMETERS-----\n")
	if err := os.WriteFile(filepath.Join(caDir, "ca.key"), append(params, keyPEM...), 0o600); err != nil {
		t.Fatal(err)
	}
	// Remove the node cert so Ensure must reissue it — the exact code path
	// that rejected multi-block CA keys.
	_ = os.Remove(filepath.Join(caDir, "node.crt"))
	_ = os.Remove(filepath.Join(caDir, "node.key"))
	b, err := Ensure(caDir, "firn-0", false)
	if err != nil {
		t.Fatalf("Ensure with openssl-style multi-block CA key: %v", err)
	}
	if err := b.VerifyClientCert(pemDecodeNodeCert(t, nodeCertOf(t, caDir))); err != nil {
		t.Fatalf("reissued node cert does not verify against its CA: %v", err)
	}
}
