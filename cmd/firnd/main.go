// firnd is the broker daemon for the firn project.
//
// Slice 6 (docs/plans/001-mvp-single-node.md): MVP hardening, on top of
// the durable file log, persisted topics, consumer groups, and queue mode.
// Both partition logs and the topic registry (topics.json) live under
// -data-dir (default ./data; "" = in-memory); a restart needs no intervention.
// Retention (delete whole segments only, ADR-003) is opt-in via
// -retention-ttl and -retention-max-bytes.
//
// Surfaces:
//
//	HTTP  :9460  /healthz, /topics, /groups, /metrics — override with -http-addr
//
// Security (ADR-011): the broker fails closed. If no API keys exist it
// prints a one-time BOOTSTRAP TOKEN on stderr; POST /apikeys with that
// token mints exactly one admin key, then auth is enforced. -dev disables
// it (development only). /metrics is keyed by default.
//
//	gRPC  :9461  FirnService                              — override with -grpc-addr
//
// Logging is JSON via log/slog on stderr.
package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/rahulkj/firn/internal/federation"
	"github.com/rahulkj/firn/internal/meshca"

	"github.com/rahulkj/firn/internal/adminui"
	"github.com/rahulkj/firn/internal/api"
	"github.com/rahulkj/firn/internal/api/firnpb"
	"github.com/rahulkj/firn/internal/apikeys"
	"github.com/rahulkj/firn/internal/audit"
	"github.com/rahulkj/firn/internal/broker"
	"github.com/rahulkj/firn/internal/groups"
	"github.com/rahulkj/firn/internal/meta"
	"github.com/rahulkj/firn/internal/metrics"
	"github.com/rahulkj/firn/internal/queue"
	"github.com/rahulkj/firn/internal/quorum"
	"github.com/rahulkj/firn/internal/ratelimit"
	"github.com/rahulkj/firn/internal/replication"
)

// meshPeerAddr maps a peer's client gRPC address to their mesh gRPC address
// (port+1, same host). The mesh listener defaults to :9471 for the client.
func meshPeerAddr(addr string) string {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return addr
	}
	return net.JoinHostPort(host, strconv.Itoa(port+10))
}

// slogEnv is slog.Info with the environment's message format.
func slogEnv(msg string, args ...any) { slog.Info(msg, args...) }

// enforceStoragePermissions walks the data dir and refuses to start unless
// every directory is 0700 (or stricter) and every regular file is 0600. A
// false flag logs an INSECURE warning and returns nil.
func enforceStoragePermissions(dir string, strict bool) error {
	if dir == "" {
		return nil
	}
	if !strict {
		slog.Warn("INSECURE: dir-mode-check disabled; any file under -data-dir may be world-readable")
		return nil
	}
	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			m := info.Mode().Perm()
			if m&0o077 != 0 {
				return fmt.Errorf("dir %s has mode %o (want 700 or stricter); refusal hint: chmod -R g-rwx,o-rwx %s", path, m, dir)
			}
			// Force dir to 0700 in case it was created before enforcing (e.g. old broker).
			if m != 0o700 {
				_ = os.Chmod(path, 0o700)
			}
			return nil
		}
		m := info.Mode().Perm()
		if m&0o077 != 0 {
			return fmt.Errorf("file %s has mode %o (want 600 or stricter)", path, m)
		}
		if m != 0o600 {
			_ = os.Chmod(path, 0o600)
		}
		return nil
	})
}

// hexToken returns a cryptographically random hex string for bootstrap use.
func hexToken(nbytes int) string {
	buf := make([]byte, nbytes)
	if _, err := rand.Read(buf); err != nil {
		panic(fmt.Sprintf("eventing/firnd: crypto/rand failed: %v", err))
	}
	return hex.EncodeToString(buf)
}

// advertisedAddr makes a listener address usable for client dialing:
// wildcard or empty hosts are replaced with localhost so single-node
// deployments advertise a reachable address in GetMetadata.
func advertisedAddr(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "localhost"
	}
	return net.JoinHostPort(host, port)
}

// dialFederationPeer builds a TLS connection to a peer cluster's mesh
// listener for FederationService calls (ADR-010, slice 2).
//
// The federation channel requires mutual TLS — the mesh listener only
// accepts clients presenting a cert signed by this cluster's mesh CA, which
// is the same CA the peer must chain to. Cross-cluster federation therefore
// assumes a shared mesh CA (single operator, dev/staging/prod). Distinct
// per-cluster CAs fail the handshake by design; the puller logs and retries.
//
// The peer record may carry its own CA (ca_crt_pem) — when set it replaces
// the local pool for *verifying the peer's server cert* (still useful when
// both clusters share the CA but the operator wants pinning to it).
// The bearer key travels as RPC metadata, attached by the puller.
func dialFederationPeer(bundle *meshca.Bundle, peer federation.Peer) (*grpc.ClientConn, error) {
	if peer.Addr == "" {
		return nil, fmt.Errorf("federation: peer %q has no addr", peer.Name)
	}
	serverName := peer.ServerName
	if serverName == "" {
		host, _, err := net.SplitHostPort(peer.Addr)
		if err != nil {
			return nil, fmt.Errorf("federation: peer %q addr %q is not host:port: %w", peer.Name, peer.Addr, err)
		}
		serverName = host
	}
	tlsCfg := bundle.ClientTLSConfig(serverName)
	if len(peer.CACrtPEM) > 0 {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(peer.CACrtPEM) {
			return nil, fmt.Errorf("federation: peer %q ca_crt_pem does not parse as a PEM CA certificate", peer.Name)
		}
		tlsCfg.RootCAs = pool
	}
	conn, err := grpc.NewClient(peer.Addr, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	if err != nil {
		return nil, fmt.Errorf("federation: dial %s: %w", peer.Addr, err)
	}
	return conn, nil
}

// chainInterceptors chains two unary interceptors into one, applied left-to-right.
func chainInterceptors(a, b grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		return a(ctx, req, info, func(ctx context.Context, req any) (resp any, err error) {
			return b(ctx, req, info, handler)
		})
	}
}

// parsePeers parses a comma-separated id=addr list.
func parsePeers(s string) (map[string]string, error) {
	out := map[string]string{}
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		idx := strings.Index(p, "=")
		if idx < 0 {
			return nil, fmt.Errorf("peer %q must be id=addr", p)
		}
		id, addr := p[:idx], p[idx+1:]
		if id == "" || addr == "" {
			return nil, fmt.Errorf("peer %q must be id=addr", p)
		}
		if _, exists := out[id]; exists {
			return nil, fmt.Errorf("duplicate peer id %q", id)
		}
		out[id] = addr
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("at least one peer is required")
	}
	return out, nil
}

const version = "0.5.0"

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))

	httpAddr := flag.String("http-addr", ":9460", "HTTP admin/health listen address")
	grpcAddr := flag.String("grpc-addr", ":9461", "gRPC listen address")
	dataDir := flag.String("data-dir", "./data", "partition log data directory (\"\" = in-memory)")
	dirModeCheck := flag.Bool("dir-mode-check", true, "refuse to start if any file under -data-dir is not 0600/dirs not 0700 (set false for exotic filesystems; logs a loud warning)")
	retentionTTL := flag.Duration("retention-ttl", 0, "delete whole segments whose newest record is older than this (0 = disabled)")
	retentionMaxBytes := flag.Int64("retention-max-bytes", 0, "max on-disk bytes per partition log; oldest whole segments deleted first (0 = disabled)")
	retentionInterval := flag.Duration("retention-interval", time.Minute, "how often the retention sweeper runs")
	segmentBytes := flag.Int64("segment-bytes", 0, "max on-disk bytes per segment per partition log (0 = broker default, 64 MiB); smaller segments make whole-segment retention actually deletable")
	blockSealInterval := flag.Duration("block-seal-interval", 0, "seal (roll) a partition's active segment once it has outlived this duration, even with no size pressure (0 = size-only sealing)")
	flushWindow := flag.Duration("flush-window", 0, "group commit (ADR-014): batch partition-log appends into windows and ack only after the window's fsync. 0 = default fsync-per-append. Example: -flush-window=2ms. Trade: up to flush-window of unacknowledged data may be lost on hard crash (never acked, but retry-until-ack is required); throughput scales with batch size")
	maxBatchBytes := flag.Int64("max-batch-bytes", 1<<20, "byte cap on a staged flush-window batch per partition (ADR-014); only used when -flush-window > 0")
	internalFlushWindow := flag.Duration("internal-flush-window", 0, "OPT-IN (never default): also apply the flush window to __consumer_offsets and __queue_attempts internal logs. On hard crash, up to this window of committed offsets may rewind (redelivery; at-least-once preserved) and delivery-attempt counts may be lost (DLQ-after-3 may trigger late). __audit/__api_keys/__federation* are NEVER windowed. Example: -internal-flush-window=2ms")
	internalMaxBatchBytes := flag.Int64("internal-max-batch-bytes", 1<<20, "byte cap on staged internal-log windows; only used when -internal-flush-window > 0")
	sessionTimeout := flag.Duration("session-timeout", 10*time.Second, "consumer group member lease; silent members are expired after this")
	nodeID := flag.String("node-id", "", "cluster mode: this node's id (cluster disabled if empty)")
	peersFlag := flag.String("peers", "", "cluster mode: comma-separated id=addr pairs, including self (e.g. n1=127.0.0.1:9301,n2=127.0.0.1:9302,n3=127.0.0.1:9303)")
	leaseTTL := flag.Duration("lease-ttl", 3*time.Second, "cluster mode: write lease TTL per partition")
	tlsCert := flag.String("tls-cert", "", "TLS certificate PEM for both HTTP and gRPC listeners (empty = plaintext)")
	tlsKey := flag.String("tls-key", "", "TLS private key PEM for both HTTP and gRPC listeners")
	dev := flag.Bool("dev", false, "DEVELOPMENT ONLY: serve without any keys (alias for -allow-open-bootstrap). WARNING: broker is open to anyone who can reach the port.")
	allowOpen := flag.Bool("allow-open-bootstrap", false, "DEVELOPMENT ONLY: serve without any keys (alias for -dev). WARNING: broker is open to anyone who can reach the port.")
	tokenBucketRate := flag.Float64("rate", 100, "per-key rate limit in operations per second")
	auditBudgetBytes := flag.Int64("audit-max-bytes", 64<<20, "max on-disk bytes for the __audit topic across this process; refuses writes past budget (fail-closed; set 0 for unlimited). Operators rotate the log manually (stop the broker, rename the segment, restart) or mount a fresh -data-dir.")
	adminHTML := flag.Bool("admin-ui", true, "serve the read-only admin UI at / on the HTTP listener")
	metricsAuth := flag.Bool("metrics-auth", true, "require a viewer key on /metrics (set false for Prometheus scrape without credentials; not recommended)")
	anonRate := flag.Int("anon-rate", 20, "max anonymous ops per -anon-window per source IP on unauthenticated endpoints (healthz, refusal attempts). 0 = unlimited.")
	federationInterval := flag.Duration("federation-interval", 500*time.Millisecond, "federation pull cadence (passive one-way mode)")
	federationBatch := flag.Int("federation-batch", 1024, "max frames per federation pull window")
	federationMaxRange := flag.Int64("federation-max-range", 1<<20, "max bytes per federation pull window")
	federationSelf := flag.String("federation-self", "", "this cluster's identity name as peers register it — sent as requesting_peer so the serving side can filter out frames this cluster donated (echo prevention, active-active). Empty disables the echo filter")
	anonWindow := flag.Duration("anon-window", time.Minute, "sliding window for -anon-rate")
	meshAddr := flag.String("mesh-addr", "", "mesh (internal) gRPC listen address — defaults to -grpc-addr with the port bumped by 10; serves only QuorumService/ReplicaService behind mTLS")
	meshCABundle := flag.String("mesh-ca-bundle", "", "pre-provisioned mesh cluster CA certificate PEM (alternative to auto-provision; required for non-loopback clusters)")
	meshCAKey := flag.String("mesh-ca-key", "", "pre-provisioned mesh cluster CA private key PEM (paired with -mesh-ca-bundle)")
	flag.Parse()

	clusterMode := *peersFlag != ""
	var peers map[string]string
	if clusterMode {
		if *dataDir == "" {
			slog.Error("cluster mode requires -data-dir (durability + replicated metadata)")
			os.Exit(1)
		}
		var err error
		peers, err = parsePeers(*peersFlag)
		if err != nil {
			slog.Error("parse -peers", "err", err)
			os.Exit(1)
		}
		if *nodeID == "" {
			slog.Error("cluster mode requires -node-id")
			os.Exit(1)
		}
		if _, ok := peers[*nodeID]; !ok {
			slog.Error("-peers must include this node's id", "node-id", *nodeID)
			os.Exit(1)
		}
	}

	var registry *meta.Registry
	var b *broker.Broker
	var keysStore *apikeys.Store
	var auditStore *audit.Store
	brokerOpts := []broker.Option{broker.WithGroupsOption(groups.WithSessionTimeout(*sessionTimeout))}
	if *segmentBytes > 0 {
		brokerOpts = append(brokerOpts, broker.WithMaxSegmentBytes(*segmentBytes))
	}
	if *blockSealInterval > 0 {
		brokerOpts = append(brokerOpts, broker.WithBlockSealing(*blockSealInterval))
	}
	if *flushWindow > 0 {
		brokerOpts = append(brokerOpts, broker.WithFlushWindow(*flushWindow, *maxBatchBytes))
		slog.Warn("group commit enabled (-flush-window): up to that window of unacknowledged produce data is at risk on hard crash; acknowledged data is always durable (ADR-014)", "flush_window", *flushWindow, "max_batch_bytes", *maxBatchBytes)
	}
	if *internalFlushWindow > 0 {
		brokerOpts = append(brokerOpts, broker.WithInternalFlushWindow(*internalFlushWindow, *internalMaxBatchBytes))
		slog.Warn("OPT-IN internal flush window enabled (-internal-flush-window): up to that window of committed offsets may rewind on hard crash (redelivery; at-least-once) and delivery-attempt counts may be lost (DLQ-after-3 may trigger late). __audit/__api_keys/__federation* remain fsync-per-append (ADR-014 slice 4)", "internal_flush_window", *internalFlushWindow, "internal_max_batch_bytes", *internalMaxBatchBytes)
	}
	if *dataDir == "" {
		registry = meta.NewRegistry()
		b = broker.New(registry, brokerOpts...)
		// In-memory broker: wire an in-memory audit store so refusals
		// never go silent — operators see them in stderr via audit fields.
		auditStore = audit.NewMemStore(*auditBudgetBytes)
	} else {
		var err error
		registry, err = meta.OpenRegistry(filepath.Join(*dataDir, "topics.json"))
		if err != nil {
			slog.Error("open topics registry", "err", err)
			os.Exit(1)
		}
		b, err = broker.NewWithDir(registry, *dataDir, brokerOpts...)
		if err != nil {
			slog.Error("open data dir", "dir", *dataDir, "err", err)
			os.Exit(1)
		}
		if fl, ok := b.InternalLog("__api_keys"); ok {
			keysStore = apikeys.NewStore(fl)
			keysStore.SetGate(b.InternalGateBox())
		}
		if fl, ok := b.InternalLog("__audit"); ok {
			auditStore = audit.NewStore(fl, *auditBudgetBytes)
			auditStore.SetGate(b.InternalGateBox())
			if *auditBudgetBytes > 0 {
				slog.Info("audit budget enabled", "bytes", *auditBudgetBytes)
			}
		}
	}

	if err := enforceStoragePermissions(*dataDir, *dirModeCheck); err != nil {
		slog.Error("storage permissions check failed", "dir", *dataDir, "err", err, "hint", "chmod -R go-rwx "+*dataDir+", or restart with -dir-mode-check=false (logs a loud warning)")
		os.Exit(1)
	}
	if *dev || *allowOpen {
		slog.Warn("INSECURE BOOTSTRAP ENABLED: authentication DISABLED (set -dev=false / -allow-open-bootstrap=false to re-enable)")
	}
	var bootstrapToken string
	switch {
	case *dev || *allowOpen:
		// open mode chosen deliberately; no token needed
	case keysStore == nil:
		bootstrapToken = hexToken(32)
	case !keysStore.HasAnyKey():
		bootstrapToken = hexToken(32)
	}
	if bootstrapToken != "" {
		slog.Warn("BOOTSTRAP MODE: broker is locked down until the first API key exists; create one with the printed BOOTSTRAP TOKEN")
		fmt.Fprintf(os.Stderr, "BOOTSTRAP TOKEN: %s\n", bootstrapToken)
	} else if !*dev && !*allowOpen {
		slog.Info("security: bearer auth enforced on all endpoints except /healthz")
	}
	authorizer := api.NewAuthorizer(keysStore, ratelimit.New(*tokenBucketRate, *tokenBucketRate), auditStore,
		api.WithOpenBootstrap(*dev || *allowOpen),
		api.WithBootstrapToken(bootstrapToken),
		api.WithAnonRate(*anonRate, *anonWindow))

	// Queue manager (slice 5): builds state for queue-mode topics and the
	// __queue_attempts internal store.
	qm, err := queue.NewManager(registry, b, *dataDir)
	if err != nil {
		slog.Error("open queue manager", "err", err)
		os.Exit(1)
	}

	// Mesh mTLS (ADR-011 slice 2): internal RPCs (ReplicaService and, in
	// cluster mode, QuorumService) move behind a separate listener that
	// only accepts peers whose cert chains to the cluster mesh CA. Client
	// surface never exposes them.
	if *meshAddr == "" {
		*meshAddr = meshPeerAddr(*grpcAddr)
	}
	meshID := *nodeID
	if meshID == "" {
		meshID = "single"
	}
	meshDir := filepath.Join(*dataDir, "mesh-ca")
	if *dataDir == "" {
		meshDir = filepath.Join(os.TempDir(), "firn-mesh-ca-"+meshID)
	}
	allowMeshAuto := !clusterMode // single-node may auto-provision; cluster mode requires a pre-provisioned shared CA

	// Explicit pre-provisioned bundle takes priority; install into meshDir.
	if *meshCABundle != "" || *meshCAKey != "" {
		if *meshCABundle == "" || *meshCAKey == "" {
			slog.Error("mesh: -mesh-ca-bundle and -mesh-ca-key must be provided together")
			os.Exit(1)
		}
		bundleBytes, err := os.ReadFile(*meshCABundle)
		if err != nil {
			slog.Error("mesh: read -mesh-ca-bundle", "err", err)
			os.Exit(1)
		}
		keyBytes, err := os.ReadFile(*meshCAKey)
		if err != nil {
			slog.Error("mesh: read -mesh-ca-key", "err", err)
			os.Exit(1)
		}
		if err := os.MkdirAll(meshDir, 0o700); err != nil {
			slog.Error("mesh: mkdir", "err", err)
			os.Exit(1)
		}
		if err := os.Chmod(meshDir, 0o700); err != nil {
			slog.Error("mesh: chmod", "err", err)
			os.Exit(1)
		}
		if err := os.WriteFile(filepath.Join(meshDir, "ca.crt"), bundleBytes, 0o600); err != nil {
			slog.Error("mesh: write ca.crt", "err", err)
			os.Exit(1)
		}
		if err := os.WriteFile(filepath.Join(meshDir, "ca.key"), keyBytes, 0o600); err != nil {
			slog.Error("mesh: write ca.key", "err", err)
			os.Exit(1)
		}
		slog.Info("mesh: installed pre-provisioned CA", "dir", meshDir)
		// Files now exist, so Ensure below just loads them.
	}

	meshBundle, err := meshca.Ensure(meshDir, meshID, allowMeshAuto)
	if err != nil {
		slog.Error("mesh: cannot ensure mesh CA", "err", err)
		os.Exit(1)
	}
	slogEnv("mesh: mTLS enabled", "addr", *meshAddr, "ca_fingerprint", meshBundle.Fingerprint())
	meshServer := grpc.NewServer(grpc.Creds(credentials.NewTLS(meshBundle.ServerTLSConfig())), grpc.MaxRecvMsgSize(80<<20), grpc.UnaryInterceptor(authorizer.MeshUnaryInterceptor()))
	firnpb.RegisterReplicaServiceServer(meshServer, api.NewReplicaServer(b))
	firnpb.RegisterRoutedBrokerServiceServer(meshServer, api.NewRoutedBrokerServer(b))

	// Cluster bootstrap (ADR-007 phases B–D): embedded TCP quorum +
	// replication agent + write-lease advisor + replica service registration.
	var ca *api.ClusterAdmin
	var qnode *quorum.Node
	if clusterMode {
		selfID := *nodeID
		names := make([]string, 0, len(peers))
		for id := range peers {
			names = append(names, id)
		}
		sort.Strings(names)
		// peerAddrs was the client gRPC list for the replication agent;
		// the mesh-translated list (meshPeerList, built below) replaced it.

		topicCreateHook := func(e quorum.Entry) {
			if e.Type != quorum.EntryTopicCreate {
				return
			}
			var t quorum.TopicMetadata
			if err := json.Unmarshal(e.Data, &t); err != nil || t.Name == "" {
				return
			}
			_ = registry.CreateWithMode(t.Name, t.Partitions, t.Mode)
		}

		var err error
		meshPeers := map[string]string{}
		for id, addr := range peers {
			meshPeers[id] = meshPeerAddr(addr)
		}
		var tr *quorum.TransportGRPC
		tr, err = quorum.NewTransportGRPC(selfID, meshPeers, meshBundle)
		if err != nil {
			slog.Error("quorum transport", "err", err)
			os.Exit(1)
		}
		qnode, err = quorum.NewNode(selfID, names, filepath.Join(*dataDir, "metadata"), tr, &quorum.NodeOptions{
			ElectionTimeout:   250 * time.Millisecond,
			HeartbeatInterval: 60 * time.Millisecond,
			ApplyHook:         topicCreateHook,
		})
		if err != nil {
			slog.Error("quorum node", "id", selfID, "err", err)
			os.Exit(1)
		}
		firnpb.RegisterQuorumServiceServer(meshServer, api.NewQuorumServer(qnode))
		qnode.Start()

		// Wire the DLQ diverter's route to the partition lease holder:
		// when the local routed produce fails (this node doesn't hold the
		// jobs.dlq partition's lease), dial the holder's mesh listener
		// (mTLS) and deliver the record there. Otherwise poison messages
		// had no path to the DLQ on a multi-node cluster and were silently
		// dropped. No-op outside cluster mode (qm route stays nil).
		qm.SetRouter(func(ctx context.Context, topic string, partition int32, key, value []byte) (int32, int64, error) {
			lease, ok := qnode.Lease(topic, partition)
			if !ok {
				return 0, 0, fmt.Errorf("quorum has no lease for %s[%d]", topic, partition)
			}
			peerAddr, ok := peers[lease.Holder]
			if !ok {
				return 0, 0, fmt.Errorf("mesh: no address for holder %q of %s[%d]", lease.Holder, topic, partition)
			}
			holderMesh := meshPeerAddr(peerAddr)
			conn, err := grpc.NewClient(holderMesh, grpc.WithTransportCredentials(credentials.NewTLS(meshBundle.ClientTLSConfig(lease.Holder))))
			if err != nil {
				return 0, 0, fmt.Errorf("mesh dial %s (holder %s): %w", holderMesh, lease.Holder, err)
			}
			defer func() { _ = conn.Close() }()
			rctx, rcancel := context.WithTimeout(ctx, 10*time.Second)
			defer rcancel()
			client := firnpb.NewRoutedBrokerServiceClient(conn)
			rresp, err := client.RoutedProduce(rctx, &firnpb.RoutedProduceRequest{Topic: topic, Key: key, Value: value})
			if err != nil {
				return 0, 0, fmt.Errorf("routed produce to %s: %w", lease.Holder, err)
			}
			return rresp.Partition, rresp.Offset, nil
		})
		// Register auto-created DLQ topics everywhere via the quorum —
		// single-node falls back to a local create.
		qm.SetTopicCreateProposer(func(name string, partitions int, mode string) error {
			data, _ := json.Marshal(quorum.TopicMetadata{Name: name, Partitions: partitions, Mode: mode, ReplicationFactor: 1})
			_, _, err := qnode.ProposeNamed(quorum.EntryTopicCreate, data)
			return err
		})

		// Replication needs each peer's MESH address plus mesh-mTLS
		// credentials. The default plaintext Dial would hit the public gRPC
		// listener, which does not serve ReplicaService (ADR-011 slice 2) —
		// dialing it surfaces "unknown service firn.v1.ReplicaService".
		meshProbe := map[string]string{}
		meshPeerList := []string{}
		for id, addr := range peers {
			meshProbe[meshPeerAddr(addr)] = id
			if id != selfID {
				meshPeerList = append(meshPeerList, meshPeerAddr(addr))
			}
		}
		sort.Strings(meshPeerList)

		agentCfg := replication.NewAgent(replication.Config{
			NodeID:     selfID,
			Peers:      meshPeerList,
			All:        names,
			Advertised: peers,
			Registry:   registry,
			Broker:     b,
			Quorum:     qnode,
			LeaseTTL:   *leaseTTL,
			DataDir:    *dataDir,
			Dial: func(ctx context.Context, addr string) (*grpc.ClientConn, error) {
				id, ok := meshProbe[addr]
				if !ok {
					return nil, fmt.Errorf("replication: peer %q is not in the membership map", addr)
				}
				return grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(meshBundle.ClientTLSConfig(id))))
			},
		})
		agentCtx, agentCancel := context.WithCancel(context.Background())
		defer agentCancel()
		go agentCfg.Run(agentCtx)
		advisor := replication.NewAdvisor(selfID, qnode, *leaseTTL)
		b.SetAdvisor(advisor)
		b.SetClusterView(agentCfg)
		b.InternalGateBox().Set(advisor.InternalGate())

		ca = &api.ClusterAdmin{
			SelfID:     selfID,
			LeaderAddr: func() string { return peers[qnode.Leader()] },
			SelfLeader: func() bool { return qnode.Role() == quorum.Leader },
			ProposeTopicCreate: func(name string, partitions int, mode string) error {
				data, _ := json.Marshal(quorum.TopicMetadata{Name: name, Partitions: partitions, Mode: mode, ReplicationFactor: 1})
				_, _, err := qnode.ProposeNamed(quorum.EntryTopicCreate, data)
				return err
			},
		}
		slog.Info("cluster mode", "node-id", selfID, "peers", peers)
	} else {
		b.SetClusterView(&replication.SingleNodeView{NodeID: "single", Addr: advertisedAddr(*grpcAddr)})
	}

	// Metrics (slice 6, ADR-006): created before federation so the puller can feed its lag gauge.
	met := metrics.New(b.Groups(), qm)

	// Seal sweep (ADR-007 blocks): with -block-seal-interval set, the
	// broker rolls aged active segments into sealed blocks on a timer. This
	// is what produces sealed blocks for replication and feeds the
	// firn_blocks_sealed_total / firn_block_bytes_sealed_total counters in
	// single-node mode where no replication agent fires the seal.
	if *blockSealInterval > 0 {
		go func() {
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()
			for range ticker.C {
				_ = b.SealAll()
			}
		}()
	}

	// ADR-007/014 wiring — Sealed counters + replication push/commit/lease
	// counters. The broker/replication packages declare the hooks; the metric
	// registry consumes them without import cycles.
	broker.SealObserver = met.ObserveSeal
	replication.RecordBlockCommit = met.ObserveBlockCommit
	replication.RecordBlockPush = met.ObserveBlockPush
	replication.RecordLeaseGrant = met.ObserveLeaseGrant
	replication.RecordLeaseRenewal = met.ObserveLeaseRenewal

	// Federation control-plane (ADR-010, slice 2): open the internal
	// shared logs and expose them to the admin handler in read/write mode.
	// Imports from any passive-mode peer start on the pull cadence.
	var federationStore *federation.Store
	if *dataDir == "" {
		federationStore = federation.OpenInMemory()
	} else {
		federationStore = federation.OpenInMemory()
		if fl, ok := b.InternalLog("__federation_peers"); ok {
			federationStore.SetPeersLog(fl)
		}
		if fl, ok := b.InternalLog("__federation_cursors"); ok {
			federationStore.SetCursorsLog(fl)
		}
	}
	// Shared passive-peer predicate used by the produce fence (broker) and
	// the queue delivery fence (qm): only PASSIVE peers fence local writes.
	// ListPeersCached folds at most once per second, so the hot append path
	// never re-folds on every write.
	passivePeer := func(peer federation.Peer) bool {
		if peer.Mode != federation.ModePassive {
			return false
		}
		return peer.Direction == federation.DirectionIn || peer.Direction == federation.DirectionBoth
	}
	// inboundPeer: passive AND active-active both count toward the advisory
	// federation watermark (min across all inbound peers, ADR-010).
	inboundPeer := func(peer federation.Peer) bool {
		if peer.Mode != federation.ModePassive && peer.Mode != federation.ModeActiveActive {
			return false
		}
		return peer.Direction == federation.DirectionIn || peer.Direction == federation.DirectionBoth
	}
	coversTopic := func(peer federation.Peer, name string) bool {
		for _, sel := range peer.Selectors {
			// An empty selector means "everything everywhere" (ADR-010).
			if len(sel.Topics) == 0 && len(sel.Queues) == 0 {
				return true
			}
			for _, t := range sel.Topics {
				if t == name {
					return true
				}
			}
			for _, q := range sel.Queues {
				if q == name {
					return true
				}
			}
		}
		return false
	}
	federatedPassive := func(name string, _ int32) bool {
		for _, peer := range federationStore.ListPeersCached(time.Second) {
			if !passivePeer(peer) {
				continue
			}
			if coversTopic(peer, name) {
				return true // passively federated here: sink, not writer
			}
		}
		return false
	}
	b.SetFederationFence(federatedPassive)
	qm.SetFederationFence(func(queue string) bool { return federatedPassive(queue, 0) })

	// Advisory federation watermark (ADR-010): FetchResponse.federation_hwm
	// is the minimum LastLocalOffset across passive peers covering the
	// topic/queue partition — 0 when unknown or unmet. A covering peer
	// without a cursor reads as 0 (min wins), matching the ADR's
	// "unknown until first import" semantics.
	b.SetFederationHWM(func(topic string, partition int32) int64 {
		minHWM := int64(-1)
		peers := federationStore.ListPeersCached(time.Second)
		cursors := federationStore.ListCursorsCached(time.Second)
		for _, peer := range peers {
			if !inboundPeer(peer) || !coversTopic(peer, topic) {
				continue
			}
			found := false
			for _, c := range cursors {
				if c.Peer == peer.Name && c.Topic == topic && c.Partition == partition {
					if minHWM == -1 || c.LastLocalOffset < minHWM {
						minHWM = c.LastLocalOffset
					}
					found = true
					break
				}
			}
			if !found {
				return 0
			}
		}
		if minHWM == -1 {
			return 0
		}
		return minHWM
	})

	// Federation puller: no-ops until a passive peer is registered; then
	// imports selected topics/partitions (and __queue_attempts) on the
	// configured cadence. It only ever reads from the peer, so an empty
	// peer set costs one timer tick.
	pullerCtx, pullerCancel := context.WithCancel(context.Background())
	defer pullerCancel()
	puller := federation.NewPuller(federation.PullerConfig{
		Broker:   b,
		Store:    federationStore,
		Interval: *federationInterval,
		Batch:    *federationBatch,
		MaxRange: *federationMaxRange,
		SelfName: *federationSelf,
		LagSeconds: func(peer, topic string, partition int32, lag float64) {
			met.FederationCursorLagSeconds.WithLabelValues(peer, topic, strconv.Itoa(int(partition))).Set(lag)
		},
		Dial: func(ctx context.Context, peer federation.Peer) (*grpc.ClientConn, error) {
			_ = ctx // grpc.NewClient is lazy; no deadline to honour at dial time
			return dialFederationPeer(meshBundle, peer)
		},
	})
	go puller.Run(pullerCtx)

	// Optional TLS on both listeners (ADR-008).
	var tlsCfg *tls.Config
	if *tlsCert != "" || *tlsKey != "" {
		if *tlsCert == "" || *tlsKey == "" {
			slog.Error("tls: both -tls-cert and -tls-key are required")
			os.Exit(1)
		}
		cert, err := tls.LoadX509KeyPair(*tlsCert, *tlsKey)
		if err != nil {
			slog.Error("tls: load key pair", "err", err)
			os.Exit(1)
		}
		tlsCfg = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	}

	// ADR-011 secure-by-default: with zero keys the broker is not serveable
	// until the operator mints one via the printed bootstrap token (or
	// HTTP admin + health + metrics + admin UI.
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		cfg := map[string]string{
			"status":  "ok",
			"version": version,
		}
		if *flushWindow > 0 {
			cfg["flushWindow"] = flushWindow.String()
		} else {
			cfg["flushWindow"] = "off"
		}
		_ = json.NewEncoder(w).Encode(cfg)
	})
	if *metricsAuth {
		mux.Handle("/metrics", authorizer.Wrap(met.Handler()))
	} else {
		mux.Handle("/metrics", met.Handler()) // scrape without key (-metrics-auth=false)
	}
	if *adminHTML {
		mux.Handle("/ui/", authorizer.Wrap(http.StripPrefix("/ui", adminui.Handler(b, federationStore))))
		mux.Handle("/ui/session", authorizer.Wrap(api.NewUISessionHandler(authorizer)))
	}
	mux.Handle("/", authorizer.Wrap(api.NewAdminHandler(registry, b.Groups(), qm, api.WithClusterAdmin(ca), api.WithAPIKeys(keysStore), api.WithAudit(auditStore), api.WithFederation(api.NewFederationSink(federationStore, api.FederationModeReadWrite)))))
	httpSrv := &http.Server{Addr: *httpAddr, Handler: api.LoggingMiddleware(api.HardeningMiddleware(mux)), ReadHeaderTimeout: 5 * time.Second}
	if tlsCfg != nil {
		httpSrv.TLSConfig = tlsCfg
	}

	// gRPC — building serverOpts this way handles single- and dual-auth
	// modes without branching at the call site.
	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		slog.Error("listen grpc", "addr", *grpcAddr, "err", err)
		os.Exit(1)
	}
	serverOpts := []grpc.ServerOption{grpc.MaxRecvMsgSize(80 << 20), grpc.UnaryInterceptor(chainInterceptors(api.LoggingUnaryInterceptor, authorizer.GRPCInterceptor()))}
	if tlsCfg != nil {
		serverOpts = append(serverOpts, grpc.Creds(credentials.NewTLS(tlsCfg)))
	}
	grpcSrv := grpc.NewServer(serverOpts...)
	firnpb.RegisterFirnServiceServer(grpcSrv, api.NewServer(b, qm, met))
	// ReplicaService and QuorumService are served ONLY on the mesh listener
	// behind mTLS (ADR-011). They are not reachable without a mesh peer cert.

	go func() {
		slog.Info("firnd starting", "version", version, "http", *httpAddr, "grpc", *grpcAddr, "data_dir", *dataDir, "tls", tlsCfg != nil)
		var listenErr error
		if tlsCfg != nil {
			listenErr = httpSrv.ListenAndServeTLS(*tlsCert, *tlsKey) // config has certs too
		} else {
			listenErr = httpSrv.ListenAndServe()
		}
		if listenErr != nil && listenErr != http.ErrServerClosed {
			slog.Error("http serve", "err", listenErr)
			os.Exit(1)
		}
	}()
	go func() {
		if err := grpcSrv.Serve(lis); err != nil {
			slog.Error("grpc serve", "err", err)
			os.Exit(1)
		}
	}()

	// Mesh listener: internal RPCs only (ADR-011 slice 2).
	meshLis, err := net.Listen("tcp", *meshAddr)
	if err != nil {
		slog.Error("listen mesh", "addr", *meshAddr, "err", err)
		os.Exit(1)
	}
	// Register FederationService on the mesh listener (mTLS-gated).
	firnpb.RegisterFederationServiceServer(meshServer, api.NewFederationServer(b, federationStore))

	go func() {
		if err := meshServer.Serve(meshLis); err != nil {
			slog.Error("mesh serve", "err", err)
			os.Exit(1)
		}
	}()

	// Session-expiry reaper for consumer groups (1s cadence, plan slice 4)
	// and queue lease expiry (slice 5).
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if n := b.Groups().ExpireSessions(); n > 0 {
				slog.Info("group reaper", "expired_members", n)
			}
			if n := qm.Expire(); n > 0 {
				slog.Info("queue reaper", "re-enqueued", n)
			}
		}
	}()

	// Retention sweeper (delete whole segments only — ADR-003).
	if *retentionTTL > 0 || *retentionMaxBytes > 0 {
		go func() {
			ticker := time.NewTicker(*retentionInterval)
			defer ticker.Stop()
			for range ticker.C {
				deleted, err := b.Retain(*retentionTTL, *retentionMaxBytes)
				if err != nil {
					slog.Error("retention sweeper", "err", err)
					continue
				}
				if deleted > 0 {
					slog.Info("retention sweeper", "deleted_segments", deleted)
				}
			}
		}()
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(ctx)
	grpcSrv.GracefulStop()
	if err := qm.Close(); err != nil {
		slog.Error("close queue manager", "err", err)
	}
	if err := b.Close(); err != nil {
		slog.Error("close broker", "err", err)
	}
	slog.Info("firnd stopped")
}
