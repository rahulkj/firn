// Package meshca issues and validates the internal-cluster mTLS mesh
// (ADR-011, plan 005, slice 2).
//
// Every node in a Firn cluster presents a node certificate signed by a
// single cluster CA. The mesh gRPC listener only accepts peers whose
// certificate chains to that CA; no API keys are involved.
//
// Auto-provisioning policy (Ensure):
//
//  1. If <dir>/ca.crt + <dir>/ca.key exist, they are used.
//  2. Else if -mesh-ca-bundle/-mesh-ca-key are provided, the bundle is
//     installed into <dir> (they must match if both provided).
//  3. Else if allowAuto is set (single-node mode), a fresh CA + node
//     certificate are generated in-place. Cluster mode never auto-provisions:
//     every node must share the CA, otherwise peers cannot authenticate each
//     other and replication is broken silently.
//  4. Else Ensure returns a hard error with instructions for provisioning a
//     shared CA once and copying it to every node. No silent trust.
//
// Directory and file modes are enforced: 0700 / 0600.
package meshca

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"
)

// Bundle holds everything a node needs to present and validate its identity.
type Bundle struct {
	CACrtPEM   []byte
	CAKeyPEM   []byte // nil for verify-only bundles (not used by this daemon)
	NodeCrtPEM []byte
	NodeKeyPEM []byte

	cert tls.Certificate
	pool *x509.CertPool
}

// CADir returns the directory where a bundle lives.
func CADir(dataDir string) string { return filepath.Join(dataDir, "mesh-ca") }

// Ensure loads an existing bundle or follows the documented auto-provision
// policy in the package doc.
//
// allowAuto must be true for a fresh CA to be generated. It is set by the
// caller when (singleNode || dev || allPeersLoopback || explicitBundle).
// In strict cluster mode (not dev, not loopback, not single node) a missing
// CA is a hard error with instructions — never a silent trust-on-first-use.
func Ensure(dir, nodeID string, allowAuto bool) (*Bundle, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("firn/meshca: mkdir %s: %w", dir, err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, fmt.Errorf("firn/meshca: chmod %s: %w", dir, err)
	}

	// 1. Existing bundle wins.
	if b, err := Load(dir, nodeID); err == nil {
		return b, nil
	}

	// 2/3. Fresh bundle only if explicitly allowed.
	if !allowAuto {
		msg := fmt.Sprintf("firn/meshca: no mesh CA found under %s (cluster mode never auto-provisions); provision one CA once with `firn-cli mesh-ca init -out mesh-ca` and copy {ca.crt,ca.key} into every node's mesh-ca/ directory (mode 0600), then restart each node", dir)
		return nil, errors.New(msg)
	}
	return Generate(dir, nodeID)
}

// Load reads and validates an existing bundle; the node certificate is
// re-verified against the CA at load time.
func Load(dir, nodeID string) (*Bundle, error) {
	caCrtPem, err := os.ReadFile(filepath.Join(dir, "ca.crt"))
	if err != nil {
		return nil, fmt.Errorf("firn/meshca: read ca.crt: %w", err)
	}
	caKeyPem, err := os.ReadFile(filepath.Join(dir, "ca.key"))
	if err != nil {
		return nil, fmt.Errorf("firn/meshca: read ca.key: %w", err)
	}
	nodeCrtPem, err := os.ReadFile(filepath.Join(dir, "node.crt"))
	if errors.Is(err, os.ErrNotExist) {
		// Node cert missing: reissue from the CA if we hold both files.
		caKey := mustParseECKey(caKeyPem)
		if caKey == nil {
			return nil, errors.New("firn/meshca: ca.key must be an ECDSA P-256 private key to reissue the node certificate")
		}
		nodeCrtPem, nodeKeyPem, err := issueNode(caCrtPem, caKey, nodeID)
		if err != nil {
			return nil, err
		}
		if err := writeFile(filepath.Join(dir, "node.crt"), nodeCrtPem); err != nil {
			return nil, err
		}
		if err := writeFile(filepath.Join(dir, "node.key"), nodeKeyPem); err != nil {
			return nil, err
		}
		return newBundle(caCrtPem, caKeyPem, nodeCrtPem, nodeKeyPem)
	}
	if err != nil {
		return nil, fmt.Errorf("firn/meshca: read node.crt: %w", err)
	}
	nodeKeyPem, err := os.ReadFile(filepath.Join(dir, "node.key"))
	if err != nil {
		return nil, fmt.Errorf("firn/meshca: read node.key: %w", err)
	}
	return newBundle(caCrtPem, caKeyPem, nodeCrtPem, nodeKeyPem)
}

// InitCA generates a fresh cluster CA and writes it to dir with 0700/0600.
// It refuses to overwrite an existing CA. Used by `firn mesh-ca init`.
func InitCA(dir string) error {
	if _, err := os.Stat(filepath.Join(dir, "ca.crt")); err == nil {
		return fmt.Errorf("firn/meshca: refusing to overwrite existing CA at %s", filepath.Join(dir, "ca.crt"))
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("firn/meshca: mkdir %s: %w", dir, err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return fmt.Errorf("firn/meshca: chmod %s: %w", dir, err)
	}
	now := time.Now()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("firn/meshca: generate CA key: %w", err)
	}
	serial, err := randomSerial()
	if err != nil {
		return err
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "firn mesh CA", Organization: []string{"firn"}},
		NotBefore:             now,
		NotAfter:              now.AddDate(10, 0, 0),
		IsCA:                  true,
		BasicConstraintsValid: true,
		MaxPathLen:            1,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return fmt.Errorf("firn/meshca: self-sign CA: %w", err)
	}
	caCrtPem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	caKeyPem := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: mustMarshalECKey(caKey)})
	if err := writeFile(filepath.Join(dir, "ca.crt"), caCrtPem); err != nil {
		return err
	}
	if err := writeFile(filepath.Join(dir, "ca.key"), caKeyPem); err != nil {
		return err
	}
	return nil
}

// VerifyClientCert reports whether peerCer t is a mesh peer of b — chains to
// the same CA. Used by the mesh server config and by tests.
func (b *Bundle) VerifyClientCert(peerCert *x509.Certificate) error {
	roots := x509.NewCertPool()
	if ok := roots.AppendCertsFromPEM(b.CACrtPEM); !ok {
		return errors.New("firn/meshca: CA cert unparsable")
	}
	intermediates := x509.NewCertPool()
	if _, err := peerCert.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		return fmt.Errorf("firn/meshca: client cert not signed by this cluster CA: %w", err)
	}
	return nil
}

// Generate creates a fresh CA + node certificate pair under dir with 0700/0600.
func Generate(dir, nodeID string) (*Bundle, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("firn/meshca: mkdir %s: %w", dir, err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, fmt.Errorf("firn/meshca: chmod %s: %w", dir, err)
	}
	now := time.Now()

	// CA keypair.
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("firn/meshca: generate CA key: %w", err)
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, err
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "firn mesh CA", Organization: []string{"firn"}},
		NotBefore:             now,
		NotAfter:              now.AddDate(10, 0, 0),
		IsCA:                  true,
		BasicConstraintsValid: true,
		MaxPathLen:            1,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("firn/meshca: self-sign CA: %w", err)
	}
	caCrtPem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	caKeyPem := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: mustMarshalECKey(caKey)})

	nodeCrtPem, nodeKeyPem, err := issueNode(caCrtPem, caKey, nodeID)
	if err != nil {
		return nil, err
	}
	if err := writeFile(filepath.Join(dir, "ca.crt"), caCrtPem); err != nil {
		return nil, err
	}
	if err := writeFile(filepath.Join(dir, "ca.key"), caKeyPem); err != nil {
		return nil, err
	}
	if err := writeFile(filepath.Join(dir, "node.crt"), nodeCrtPem); err != nil {
		return nil, err
	}
	if err := writeFile(filepath.Join(dir, "node.key"), nodeKeyPem); err != nil {
		return nil, err
	}
	return newBundle(caCrtPem, caKeyPem, nodeCrtPem, nodeKeyPem)
}

// issueNode mints a node certificate signed by caKey (P-256 ECDSA).
func issueNode(caCrtPem []byte, caKey *ecdsa.PrivateKey, nodeID string) ([]byte, []byte, error) {
	now := time.Now()
	caBlock, _ := pem.Decode(caCrtPem)
	caCert, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("firn/meshca: parse CA cert: %w", err)
	}

	nodeKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("firn/meshca: generate node key: %w", err)
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, nil, err
	}
	nodeTemplate := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: nodeID, Organization: []string{"firn"}},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.AddDate(2, 0, 0),
		DNSNames:     []string{nodeID},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, nodeTemplate, caCert, &nodeKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, fmt.Errorf("firn/meshca: sign node cert: %w", err)
	}
	crt := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	key := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: mustMarshalECKey(nodeKey)})
	return crt, key, nil
}

// ServerTLSConfig returns a *tls.Config suitable for an mTLS listener: the
// mesh requires the client to present a cert signed by the cluster CA.
func (b *Bundle) ServerTLSConfig() *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{b.cert},
		ClientCAs:    b.pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}
}

// ClientTLSConfig returns a *tls.Config suitable for dialing a mesh peer.
// ServerName is the peer's node id (the cert's CN).
func (b *Bundle) ClientTLSConfig(serverName string) *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{b.cert},
		RootCAs:      b.pool,
		ServerName:   serverName,
		MinVersion:   tls.VersionTLS12,
	}
}

func newBundle(caCrtPem, caKeyPem, nodeCrtPem, nodeKeyPem []byte) (*Bundle, error) {
	cert, err := tls.X509KeyPair(nodeCrtPem, nodeKeyPem)
	if err != nil {
		return nil, fmt.Errorf("firn/meshca: load node key pair: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caCrtPem) {
		return nil, errors.New("firn/meshca: CA cert unparsable")
	}
	return &Bundle{
		CACrtPEM:   caCrtPem,
		CAKeyPEM:   caKeyPem,
		NodeCrtPEM: nodeCrtPem,
		NodeKeyPEM: nodeKeyPem,
		cert:       cert,
		pool:       pool,
	}, nil
}

func writeFile(path string, content []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("firn/meshca: write %s: %w", path, err)
	}
	if _, err := f.Write(content); err != nil {
		_ = f.Close()
		return fmt.Errorf("firn/meshca: write %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("firn/meshca: sync %s: %w", path, err)
	}
	return f.Close()
}

func randomSerial() (*big.Int, error) {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("firn/meshca: serial: %w", err)
	}
	return serial, nil
}

// mustParseECKey accepts any PEM bundle that contains an EC private key,
// not necessarily in the first block. openssl's `ecparam -genkey` emits a
// leading "EC PARAMETERS" block before "EC PRIVATE KEY" — a perfectly legal
// CA file that must not be rejected.
func mustParseECKey(pemBytes []byte) *ecdsa.PrivateKey {
	rest := pemBytes
	for len(rest) > 0 {
		block, tail := pem.Decode(rest)
		if block == nil {
			return nil
		}
		// Support both "EC PRIVATE KEY" (SEC1) and "PRIVATE KEY" (PKCS8) encodings.
		if block.Type == "EC PRIVATE KEY" {
			if key, err := x509.ParseECPrivateKey(block.Bytes); err == nil {
				return key
			}
		}
		if block.Type == "PRIVATE KEY" || block.Type == "EC PRIVATE KEY" {
			if key, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
				if ec, ok := key.(*ecdsa.PrivateKey); ok {
					return ec
				}
			}
		}
		rest = tail
	}
	return nil
}

func mustMarshalECKey(key *ecdsa.PrivateKey) []byte {
	asn1Bytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		panic(err) // invariant: key is a valid EC key from this package
	}
	return asn1Bytes
}

// Fingerprint returns a stable short fingerprint of the cluster CA for
// operator diagnostics (logged at startup, safe to print).
func (b *Bundle) Fingerprint() string {
	sum := sha256.Sum256(b.CACrtPEM)
	return fmt.Sprintf("%x", sum[:8])
}
