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
//	HTTP  :9092  /healthz, /topics, /groups, /metrics (admin) — override with -http-addr
//	gRPC  :9093  FirnService                              — override with -grpc-addr
//
// Logging is JSON via log/slog on stderr.
package main

import (
	"context"
	"crypto/tls"
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
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

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

const version = "0.3.0"

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))

	httpAddr := flag.String("http-addr", ":9092", "HTTP admin/health listen address")
	grpcAddr := flag.String("grpc-addr", ":9093", "gRPC listen address")
	dataDir := flag.String("data-dir", "./data", "partition log data directory (\"\" = in-memory)")
	retentionTTL := flag.Duration("retention-ttl", 0, "delete whole segments whose newest record is older than this (0 = disabled)")
	retentionMaxBytes := flag.Int64("retention-max-bytes", 0, "max on-disk bytes per partition log; oldest whole segments deleted first (0 = disabled)")
	retentionInterval := flag.Duration("retention-interval", time.Minute, "how often the retention sweeper runs")
	segmentBytes := flag.Int64("segment-bytes", 0, "max on-disk bytes per segment per partition log (0 = broker default, 64 MiB); smaller segments make whole-segment retention actually deletable")
	blockSealInterval := flag.Duration("block-seal-interval", 0, "seal (roll) a partition's active segment once it has outlived this duration, even with no size pressure (0 = size-only sealing)")
	sessionTimeout := flag.Duration("session-timeout", 10*time.Second, "consumer group member lease; silent members are expired after this")
	nodeID := flag.String("node-id", "", "cluster mode: this node's id (cluster disabled if empty)")
	peersFlag := flag.String("peers", "", "cluster mode: comma-separated id=addr pairs, including self (e.g. n1=127.0.0.1:9301,n2=127.0.0.1:9302,n3=127.0.0.1:9303)")
	leaseTTL := flag.Duration("lease-ttl", 3*time.Second, "cluster mode: write lease TTL per partition")
	tlsCert := flag.String("tls-cert", "", "TLS certificate PEM for both HTTP and gRPC listeners (empty = plaintext)")
	tlsKey := flag.String("tls-key", "", "TLS private key PEM for both HTTP and gRPC listeners")
	tokenBucketRate := flag.Float64("rate", 100, "per-key rate limit in operations per second")
	adminHTML := flag.Bool("admin-ui", true, "serve the read-only admin UI at / on the HTTP listener")
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
	if *dataDir == "" {
		registry = meta.NewRegistry()
		b = broker.New(registry, brokerOpts...)
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
			auditStore = audit.NewStore(fl)
			auditStore.SetGate(b.InternalGateBox())
		}
	}

	// Queue manager (slice 5): builds state for queue-mode topics and the
	// __queue_attempts internal store.
	qm, err := queue.NewManager(registry, b, *dataDir)
	if err != nil {
		slog.Error("open queue manager", "err", err)
		os.Exit(1)
	}

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
		peerAddrs := []string{}
		for id, a := range peers {
			if id != selfID {
				peerAddrs = append(peerAddrs, a)
			}
		}
		sort.Strings(peerAddrs)

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
		var tr *quorum.TransportGRPC
		tr, err = quorum.NewTransportGRPC(selfID, peers)
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
		qnode.Start()

		agentCfg := replication.NewAgent(replication.Config{
			NodeID:     selfID,
			Peers:      peerAddrs,
			All:        names,
			Advertised: peers,
			Registry:   registry,
			Broker:     b,
			Quorum:     qnode,
			LeaseTTL:   *leaseTTL,
			DataDir:    *dataDir,
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

	// Metrics (slice 6, ADR-006).
	met := metrics.New(b.Groups(), qm)

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

	// Auth (ADR-008): open when no keys are configured; otherwise bearer
	// token required, rate-limited per key, role-gated at the boundary.
	authorizer := api.NewAuthorizer(keysStore, ratelimit.New(*tokenBucketRate, *tokenBucketRate), auditStore)

	// HTTP admin + health + metrics + admin UI.
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok", "version": version})
	})
	mux.Handle("/metrics", met.Handler())
	if *adminHTML {
		mux.Handle("/ui/", authorizer.Wrap(http.StripPrefix("/ui", adminui.Handler(b))))
	}
	mux.Handle("/", authorizer.Wrap(api.NewAdminHandler(registry, b.Groups(), qm, api.WithClusterAdmin(ca), api.WithAPIKeys(keysStore), api.WithAudit(auditStore))))
	httpSrv := &http.Server{Addr: *httpAddr, Handler: api.LoggingMiddleware(mux), ReadHeaderTimeout: 5 * time.Second}
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
	firnpb.RegisterReplicaServiceServer(grpcSrv, api.NewReplicaServer(b))
	if clusterMode {
		firnpb.RegisterQuorumServiceServer(grpcSrv, api.NewQuorumServer(qnode))
	}

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
