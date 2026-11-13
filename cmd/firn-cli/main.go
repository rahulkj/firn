// firn-cli is the operator/admin tool for the firn broker.
//
// Admin (create/list topics, groups) goes over HTTP per ADR-002; data-plane
// operations (produce/fetch/dlq) go over gRPC.
//
// Usage:
//
//	firn-cli [--addr localhost:9093] [--http-addr localhost:9092] [--json] <command> [flags]
//
// Commands:
//
//	create-topic --name T [--partitions N] [--mode stream|queue]
//	create-queue --name Q [--partitions N]                 (mode=queue)
//	list                                                   list topics
//	produce --topic T [--key K] [--value V]                produce a message (value "-" reads stdin)
//	fetch   --topic T [--partition P] [--offset O] [--max N]
//	groups                                                 list consumer groups
//	dlq list    --queue Q                                  list messages in <Q>.dlq
//	dlq redrive --queue Q                                  re-produce DLQ messages into the source queue
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/rahulkj/firn/internal/api/firnpb"
	"github.com/rahulkj/firn/internal/meshca"
)

var (
	addr      = flag.String("addr", envOr("FIRN_ADDR", "localhost:9093"), "gRPC address of a node (or FIRN_ADDR)")
	httpAddr  = flag.String("http-addr", envOr("FIRN_HTTP_ADDR", "localhost:9092"), "HTTP admin address (or FIRN_HTTP_ADDR)")
	peers     = flag.String("peers", envOr("FIRN_PEERS", ""), "cluster mode: comma-separated node gRPC addresses (route decisions come from the freshest metadata)")
	httpPeers = flag.String("http-peers", envOr("FIRN_HTTP_PEERS", ""), "cluster mode: comma-separated node HTTP admin addresses (default: -peers with each port decremented by 1)")
	jsonOut   = flag.Bool("json", false, "machine-readable JSON output")
	apiKey    = flag.String("api-key", envOr("FIRN_API_KEY", ""), "API key for bearer auth (or FIRN_API_KEY)")
	caCert    = flag.String("ca-cert", envOr("FIRN_CA_CERT", ""), "CA certificate for TLS (or FIRN_CA_CERT); if empty, plaintext insecure dialing is used")
)

var (
	clusterAddrs     []string // from -peers; empty in single-node mode
	clusterHTTPAddrs []string // from -http-peers (or derived from -peers)
)

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// initCluster parses -peers/-http-peers and derives the HTTP list from the
// gRPC list if not provided (each port decremented by 1, matching the
// default firnd layout).
func initCluster() {
	if *peers == "" {
		return
	}
	for _, p := range strings.Split(*peers, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			clusterAddrs = append(clusterAddrs, p)
		}
	}
	if *httpPeers == "" {
		for _, a := range clusterAddrs {
			host, portStr, err := net.SplitHostPort(a)
			if err != nil {
				clusterHTTPAddrs = append(clusterHTTPAddrs, a)
				continue
			}
			port, err := strconv.Atoi(portStr)
			if err != nil {
				clusterHTTPAddrs = append(clusterHTTPAddrs, a)
				continue
			}
			clusterHTTPAddrs = append(clusterHTTPAddrs, net.JoinHostPort(host, strconv.Itoa(port-1)))
		}
	} else {
		for _, p := range strings.Split(*httpPeers, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				clusterHTTPAddrs = append(clusterHTTPAddrs, p)
			}
		}
	}
}

func main() {
	flag.Parse()
	initCluster()
	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}
	var err error
	switch args[0] {
	case "create-topic":
		err = cmdCreateTopic(args[1:], "stream")
	case "create-queue":
		err = cmdCreateTopic(args[1:], "queue")
	case "list":
		err = cmdList()
	case "create-apikey":
		err = cmdCreateAPIKey(args[1:])
	case "revoke-apikey":
		err = cmdRevokeAPIKey(args[1:])
	case "list-apikeys":
		err = cmdListAPIKeys()
	case "mesh-ca":
		err = cmdMeshCA(args[1:])
	case "produce":
		err = cmdProduce(args[1:])
	case "fetch":
		err = cmdFetch(args[1:])
	case "groups":
		err = cmdGroups()
	case "dlq":
		err = cmdDLQ(args[1:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "firn-cli: %v\n", err)
		os.Exit(1)
	}
}

// isFencedResponse reports whether a response indicates the internal topic is
// fenced on that node (HTTP 503 with the fence marker), meaning another node
// holds the internal lease and the CLI should try the next seed.
func isFencedResponse(code int, body []byte) bool {
	return code == http.StatusServiceUnavailable &&
		(bytes.Contains(body, []byte("fenced")) || bytes.Contains(body, []byte("held by")) || bytes.Contains(body, []byte("lease")))
}

// doSeedCall runs op against each seed until one succeeds without a fence
// error. Transport errors and fence rejections fall through to the next
// seed; deterministic failures (4xx validation errors) return immediately.
func doSeedCall(op func(seed string) (int, []byte, error)) (int, []byte, error) {
	seeds := clusterHTTPAddrs
	if len(seeds) == 0 {
		seeds = []string{*httpAddr}
	}
	var lastCode int
	var lastBody []byte
	var lastErr error
	for _, seed := range seeds {
		code, body, err := op(seed)
		if err != nil {
			lastErr = err
			continue
		}
		if isFencedResponse(code, body) {
			lastCode, lastBody, lastErr = code, body, fmt.Errorf("internal topic fenced on %s (HTTP 503)", seed)
			continue
		}
		return code, body, nil
	}
	if lastErr != nil {
		return lastCode, lastBody, lastErr
	}
	return 0, nil, fmt.Errorf("all seed nodes failed")
}

func cmdCreateAPIKey(args []string) error {
	fs := flag.NewFlagSet("create-apikey", flag.ContinueOnError)
	name := fs.String("name", "", "key name (required)")
	role := fs.String("role", "operator", "role: viewer|operator|admin")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return fmt.Errorf("--name is required")
	}
	code, body, err := doSeedCall(func(seed string) (int, []byte, error) {
		req, err := http.NewRequest("POST", httpScheme()+"://"+seed+"/apikeys", bytes.NewReader([]byte(fmt.Sprintf(`{"name":%q,"role":%q}`, *name, *role))))
		if err != nil {
			return 0, nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		if *apiKey != "" {
			req.Header.Set("Authorization", "Bearer "+*apiKey)
		}
		resp, err := httpClient().Do(req)
		if err != nil {
			return 0, nil, err
		}
		defer func() { _ = resp.Body.Close() }()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, body, nil
	})
	if err != nil {
		return fmt.Errorf("create-apikey: %w", err)
	}
	if code != http.StatusCreated {
		return fmt.Errorf("create-apikey: HTTP %d: %s", code, body)
	}
	var out struct {
		Key  string `json:"key"`
		Name string `json:"name"`
		Role string `json:"role"`
	}
	_ = json.Unmarshal(body, &out)
	if *jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(map[string]string{"name": out.Name, "role": out.Role, "key": out.Key})
	}
	fmt.Printf("created api key %q (role: %s): %s\n", out.Name, out.Role, out.Key)
	fmt.Printf("note: the key is shown exactly once; store it now.\n")
	return nil
}

func cmdRevokeAPIKey(args []string) error {
	fs := flag.NewFlagSet("revoke-apikey", flag.ContinueOnError)
	name := fs.String("name", "", "key name (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return fmt.Errorf("--name is required")
	}
	code, body, err := doSeedCall(func(seed string) (int, []byte, error) {
		req, err := http.NewRequest("DELETE", httpScheme()+"://"+seed+"/apikeys/"+*name, nil)
		if err != nil {
			return 0, nil, err
		}
		if *apiKey != "" {
			req.Header.Set("Authorization", "Bearer "+*apiKey)
		}
		resp, err := httpClient().Do(req)
		if err != nil {
			return 0, nil, err
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, b, nil
	})
	if err != nil {
		return fmt.Errorf("revoke-apikey: %w", err)
	}
	if code != http.StatusOK {
		return fmt.Errorf("revoke-apikey: HTTP %d: %s", code, body)
	}
	fmt.Printf("revoked api key %q\n", *name)
	return nil
}

func cmdListAPIKeys() error {
	httpAddr := *httpAddr
	if peers := clusterHTTPAddrs; len(peers) > 0 {
		httpAddr = peers[0]
	}
	req, err := http.NewRequest("GET", httpScheme()+"://"+httpAddr+"/apikeys", nil)
	if err != nil {
		return err
	}
	if *apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+*apiKey)
	}
	resp, err := httpClient().Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("list-apikeys: HTTP %d: %s", resp.StatusCode, body)
	}
	var out struct {
		Keys []struct {
			Name      string `json:"name"`
			Role      string `json:"role"`
			Revoked   bool   `json:"revoked"`
			CreatedAt int64  `json:"createdAt"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return err
	}
	if *jsonOut {
		return printJSON(out)
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "NAME\tROLE\tREVOKED")
	for _, k := range out.Keys {
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%t\n", k.Name, k.Role, k.Revoked)
	}
	return tw.Flush()
}

func cmdMeshCA(args []string) error {
	// Accept both `firn-cli mesh-ca` and `firn-cli mesh-ca init`.
	if len(args) > 0 && args[0] == "init" {
		args = args[1:]
	}
	fs := flag.NewFlagSet("mesh-ca", flag.ContinueOnError)
	outDir := fs.String("out", "mesh-ca", "directory to write ca.crt and ca.key")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := os.MkdirAll(*outDir, 0o700); err != nil {
		return fmt.Errorf("mesh-ca init: mkdir %s: %w", *outDir, err)
	}
	if err := meshca.InitCA(*outDir); err != nil {
		return err
	}
	fmt.Printf("mesh CA written under %s (ca.crt, ca.key)\n", *outDir)
	fmt.Printf("copy {ca.crt,ca.key} into every node's <data-dir>/mesh-ca/ (mode 0600)\n")
	return nil
}

func usage() {
	fmt.Fprint(os.Stderr, `usage: firn-cli [--addr gRPC] [--http-addr HTTP] [--api-key KEY] [--ca-cert PEM] [--json] <command> [flags]

commands: create-topic create-queue list produce fetch groups dlq create-apikey revoke-apikey list-apikeys mesh-ca
see -h on any command for its flags (e.g. firn-cli produce -h)
`)
}

// fnv32a hashes b the way the broker partitions (hash/fnv.New32a).
func fnv32a(b []byte) uint32 {
	h := uint32(2166136261)
	for _, c := range b {
		h *= 16777619
		h ^= uint32(c)
	}
	return h
}

func tlsConfigForCA() (*tls.Config, error) {
	pool, err := x509.SystemCertPool()
	if err != nil {
		pool = x509.NewCertPool()
	}
	pem, err := os.ReadFile(*caCert)
	if err != nil {
		return nil, err
	}
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no CA certs appended from %s", *caCert)
	}
	return &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}, nil
}

func grpcClientTo(peer string) (firnpb.FirnServiceClient, *grpc.ClientConn, error) {
	creds := insecure.NewCredentials()
	if *caCert != "" {
		cfg, err := tlsConfigForCA()
		if err != nil {
			return nil, nil, err
		}
		creds = credentials.NewTLS(cfg)
	}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(creds)}
	if *apiKey != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(bearerCreds{token: *apiKey}))
	}
	conn, err := grpc.NewClient(peer, opts...)
	if err != nil {
		return nil, nil, err
	}
	return firnpb.NewFirnServiceClient(conn), conn, nil
}

type bearerCreds struct{ token string }

func (c bearerCreds) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + c.token}, nil
}

func (c bearerCreds) RequireTransportSecurity() bool { return *caCert != "" }

func clusterSeedAddrs() []string {
	if len(clusterAddrs) == 0 {
		return []string{*addr}
	}
	return clusterAddrs
}

type metaCache struct {
	mu   sync.Mutex
	resp *firnpb.GetMetadataResponse
	hold time.Time
}

var cache metaCache

// metadata fetches (cached, 2s TTL) the routing view from any responsive node.
func metadata(ctx context.Context, force bool) (*firnpb.GetMetadataResponse, error) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if !force && cache.resp != nil && time.Now().Before(cache.hold) {
		return cache.resp, nil
	}
	var lastErr error
	for _, peer := range clusterSeedAddrs() {
		client, conn, err := grpcClientTo(peer)
		if err != nil {
			lastErr = err
			continue
		}
		mdCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		md, err := client.GetMetadata(mdCtx, &firnpb.GetMetadataRequest{})
		cancel()
		_ = conn.Close()
		if err != nil {
			lastErr = err
			continue
		}
		cache.resp = md
		cache.hold = time.Now().Add(2 * time.Second)
		return md, nil
	}
	return nil, fmt.Errorf("no node returned metadata: %v", lastErr)
}

// routeAddr picks a node address to talk to for (topic, partition).
// Order of preference: lease holder from metadata; else any listed broker.
func routeAddr(ctx context.Context, topic string, partition int32, refresh bool) string {
	md, err := metadata(ctx, refresh)
	if err == nil {
		for _, t := range md.Topics {
			if t.Name != topic {
				continue
			}
			for _, p := range t.PartitionsMeta {
				if p.PartitionId == partition && p.LeaderAddr != "" {
					return p.LeaderAddr
				}
			}
		}
		for _, b := range md.Brokers {
			if b.Addr != "" {
				return b.Addr
			}
		}
	}
	// Fall back to seed list round-robin.
	seeds := clusterSeedAddrs()
	if len(seeds) == 0 {
		return *addr
	}
	return seeds[0]
}

// httpScheme picks https when a CA cert is configured, http otherwise.
func httpScheme() string {
	if *caCert != "" {
		return "https"
	}
	return "http"
}

func httpClient() *http.Client {
	if *caCert != "" {
		if cfg, err := tlsConfigForCA(); err == nil {
			return &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{TLSClientConfig: cfg}}
		}
	}
	return &http.Client{Timeout: 10 * time.Second}
}

func bearer(req *http.Request) {
	if *apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+*apiKey)
	}
}

func httpGet(path string, out any) (int, error) {
	req, err := http.NewRequest("GET", httpScheme()+"://"+*httpAddr+path, nil)
	if err != nil {
		return 0, err
	}
	bearer(req)
	resp, err := httpClient().Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	data, _ := io.ReadAll(resp.Body)
	if out != nil {
		_ = json.Unmarshal(data, out)
	}
	return resp.StatusCode, nil
}

func isRetryable(err error) bool {
	code := status.Code(err)
	if code == codes.Unavailable {
		return true
	}
	msg := err.Error()
	// Lease-holder mismatch: another node is the writer for this partition.
	return strings.Contains(msg, "not lease holder") || strings.Contains(msg, "failed precondition") || code == codes.FailedPrecondition
}

// callWithRouting routes a gRPC call to the lease holder for (topic,
// partition), retrying with metadata refresh on retryable errors. When
// partition < 0 (keyless produce), it tries seed nodes in order.
func callWithRouting(ctx context.Context, topic string, partition int32, call func(firnpb.FirnServiceClient) error) error {
	var lastErr error
	attempt := 0
	for attempt <= len(clusterSeedAddrs()) {
		var addr string
		if partition >= 0 {
			addr = routeAddr(ctx, topic, partition, attempt > 0)
		} else {
			addr = clusterSeedAddrs()[attempt%len(clusterSeedAddrs())]
		}
		client, conn, err := grpcClientTo(addr)
		if err == nil {
			err = call(client)
		}
		_ = conn.Close()
		if err == nil || !isRetryable(err) {
			return err
		}
		lastErr = err
		attempt++
	}
	return lastErr
}

func cmdCreateTopic(args []string, mode string) error {
	fs := flag.NewFlagSet("create-topic", flag.ContinueOnError)
	name := fs.String("name", "", "topic name (required)")
	partitions := fs.Int("partitions", 1, "partition count")
	modeFlag := fs.String("mode", mode, "topic mode: stream|queue")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return fmt.Errorf("--name is required")
	}
	m := *modeFlag
	if mode == "queue" {
		m = "queue"
	}

	tryCreate := func(httpAddr string) (int, error) {
		buf, _ := json.Marshal(map[string]any{"name": *name, "partitions": *partitions, "mode": m})
		req, err := http.NewRequest("POST", httpScheme()+"://"+httpAddr+"/topics", bytes.NewReader(buf))
		if err != nil {
			return 0, err
		}
		req.Header.Set("Content-Type", "application/json")
		if *apiKey != "" {
			req.Header.Set("Authorization", "Bearer "+*apiKey)
		}
		resp, err := httpClient().Do(req)
		if err != nil {
			return 0, err
		}
		defer func() { _ = resp.Body.Close() }()
		_, _ = io.ReadAll(resp.Body)
		return resp.StatusCode, nil
	}

	if len(clusterHTTPAddrs) == 0 {
		code, err := tryCreate(*httpAddr)
		if err != nil {
			return err
		}
		if code == http.StatusConflict {
			return fmt.Errorf("topic %q already exists", *name)
		}
		if code != http.StatusCreated && code != http.StatusAccepted {
			return fmt.Errorf("create topic: HTTP %d", code)
		}
	} else {
		// Cluster mode: try every admin node until one accepts (the leader).
		created := false
		var lastCode int
		for _, ha := range clusterHTTPAddrs {
			code, err := tryCreate(ha)
			if err != nil {
				lastCode = 0
				continue
			}
			if code == http.StatusCreated || code == http.StatusAccepted {
				created = true
				lastCode = code
				break
			}
			lastCode = code
		}
		if !created {
			return fmt.Errorf("create topic: no node accepted (last HTTP %d); is -peers/-http-peers correct?", lastCode)
		}
	}
	if *jsonOut {
		fmt.Printf(`{"name":%q,"partitions":%d,"mode":%q}`+"\n", *name, *partitions, m)
		return nil
	}
	fmt.Printf("created topic %q (partitions=%d mode=%s)\n", *name, *partitions, m)
	return nil
}

func cmdList() error {
	var out struct {
		Topics []struct {
			Name       string `json:"name"`
			Partitions int    `json:"partitions"`
			Mode       string `json:"mode"`
		} `json:"topics"`
	}
	code, err := httpGet("/topics", &out)
	if err != nil {
		return err
	}
	if code != http.StatusOK {
		return fmt.Errorf("list: HTTP %d", code)
	}
	if *jsonOut {
		return printJSON(out)
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "NAME\tPARTITIONS\tMODE")
	for _, t := range out.Topics {
		_, _ = fmt.Fprintf(tw, "%s\t%d\t%s\n", t.Name, t.Partitions, t.Mode)
	}
	return tw.Flush()
}

func cmdProduce(args []string) error {
	fs := flag.NewFlagSet("produce", flag.ContinueOnError)
	topic := fs.String("topic", "", "topic (required)")
	key := fs.String("key", "", "message key (optional)")
	value := fs.String("value", "", "message value; '-' reads stdin")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *topic == "" || *value == "" {
		return fmt.Errorf("--topic and --value are required")
	}
	v := *value
	if v == "-" {
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		v = string(data)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	keyBytes := []byte(*key)
	if *key == "" {
		keyBytes = nil
	}
	partition := int32(-1)
	if len(keyBytes) > 0 {
		// Precompute the partition the broker will assign (broker uses
		// fnv32a(key) % partitions; keyless routing retries seeds in order).
		md, err := metadata(ctx, false)
		if err == nil {
			for _, t := range md.Topics {
				if t.Name == *topic && t.Partitions > 0 {
					partition = int32(fnv32a(keyBytes) % uint32(t.Partitions))
				}
			}
		}
	}
	var resp *firnpb.ProduceResponse
	err := callWithRouting(ctx, *topic, partition, func(client firnpb.FirnServiceClient) error {
		r, err := client.Produce(ctx, &firnpb.ProduceRequest{Topic: *topic, Key: keyBytes, Value: []byte(v)})
		resp = r
		return err
	})
	if err != nil {
		return err
	}
	if *jsonOut {
		return printJSON(map[string]any{"partition": resp.Partition, "offset": resp.Offset})
	}
	fmt.Printf("produced to %s partition=%d offset=%d\n", *topic, resp.Partition, resp.Offset)
	return nil
}

func cmdFetch(args []string) error {
	fs := flag.NewFlagSet("fetch", flag.ContinueOnError)
	topic := fs.String("topic", "", "topic (required)")
	partition := fs.Int("partition", 0, "partition")
	offset := fs.Int64("offset", 0, "start offset")
	max := fs.Int("max", 100, "max records")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *topic == "" {
		return fmt.Errorf("--topic is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var resp *firnpb.FetchResponse
	err := callWithRouting(ctx, *topic, int32(*partition), func(client firnpb.FirnServiceClient) error {
		r, err := client.Fetch(ctx, &firnpb.FetchRequest{Topic: *topic, Partition: int32(*partition), Offset: *offset, MaxRecords: int32(*max)})
		resp = r
		return err
	})
	if err != nil {
		return err
	}
	if *jsonOut {
		out := []map[string]any{}
		for _, r := range resp.Records {
			out = append(out, map[string]any{"offset": r.Offset, "key": string(r.Key), "value": string(r.Value)})
		}
		return printJSON(map[string]any{"records": out})
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "OFFSET\tKEY\tVALUE")
	for _, r := range resp.Records {
		_, _ = fmt.Fprintf(tw, "%d\t%s\t%s\n", r.Offset, string(r.Key), string(r.Value))
	}
	return tw.Flush()
}

func cmdGroups() error {
	var out struct {
		Groups []struct {
			Name       string `json:"name"`
			Generation int64  `json:"generation"`
			Leader     string `json:"leader"`
			Protocol   string `json:"protocol"`
			Members    []struct {
				ID         string             `json:"id"`
				Topics     []string           `json:"topics"`
				Assignment map[string][]int32 `json:"assignment"`
			} `json:"members"`
		} `json:"groups"`
	}
	code, err := httpGet("/groups", &out)
	if err != nil {
		return err
	}
	if code != http.StatusOK {
		return fmt.Errorf("groups: HTTP %d", code)
	}
	if *jsonOut {
		return printJSON(out)
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "GROUP\tGENERATION\tLEADER\tMEMBERS")
	for _, g := range out.Groups {
		members := []string{}
		for _, m := range g.Members {
			members = append(members, m.ID)
		}
		_, _ = fmt.Fprintf(tw, "%s\t%d\t%s\t%s\n", g.Name, g.Generation, g.Leader, strings.Join(members, ","))
	}
	return tw.Flush()
}

func dlqFetchAll(ctx context.Context, dlq string) ([]*firnpb.Record, error) {
	var out []*firnpb.Record
	off := int64(0)
	for {
		var resp *firnpb.FetchResponse
		err := callWithRouting(ctx, dlq, 0, func(client firnpb.FirnServiceClient) error {
			r, err := client.Fetch(ctx, &firnpb.FetchRequest{Topic: dlq, Partition: 0, Offset: off, MaxRecords: 256})
			resp = r
			return err
		})
		if err != nil {
			return nil, err
		}
		if len(resp.Records) == 0 { // offset == high watermark: done
			return out, nil
		}
		for _, r := range resp.Records {
			off = r.Offset + 1
		}
		out = append(out, resp.Records...)
	}
}

func cmdDLQ(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("dlq needs a subcommand: list|redrive")
	}
	switch args[0] {
	case "list":
		fs := flag.NewFlagSet("dlq list", flag.ContinueOnError)
		name := fs.String("queue", "", "queue name (required)")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if *name == "" {
			return fmt.Errorf("--queue is required")
		}
		queue := *name
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		recs, err := dlqFetchAll(ctx, queue+".dlq")
		if err != nil {
			return err
		}
		if *jsonOut {
			out := []map[string]any{}
			for _, r := range recs {
				out = append(out, map[string]any{"offset": r.Offset, "key": string(r.Key), "value": string(r.Value)})
			}
			return printJSON(map[string]any{"queue": queue, "records": out})
		}
		tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "OFFSET\tKEY\tVALUE")
		for _, r := range recs {
			_, _ = fmt.Fprintf(tw, "%d\t%s\t%s\n", r.Offset, string(r.Key), string(r.Value))
		}
		return tw.Flush()

	case "redrive":
		fs := flag.NewFlagSet("dlq redrive", flag.ContinueOnError)
		name := fs.String("queue", "", "queue name (required)")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if *name == "" {
			return fmt.Errorf("--queue is required")
		}
		queue := *name
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		recs, err := dlqFetchAll(ctx, queue+".dlq")
		if err != nil {
			return err
		}
		for _, r := range recs {
			rec := r
			err := callWithRouting(ctx, queue, -1, func(client firnpb.FirnServiceClient) error {
				_, err := client.Produce(ctx, &firnpb.ProduceRequest{Topic: queue, Key: rec.Key, Value: rec.Value})
				return err
			})
			if err != nil {
				return fmt.Errorf("redrive offset %d: %w", rec.Offset, err)
			}
		}
		// Note: the DLQ is not modified; records remain readable (offsets are
		// never reused or deleted in v1). Attempt counts reset naturally
		// because re-driven messages get fresh offsets and attempt keys.
		if *jsonOut {
			return printJSON(map[string]any{"queue": queue, "redriven": len(recs)})
		}
		fmt.Printf("redrived %d message(s) from %s.dlq to %s\n", len(recs), queue, queue)
		return nil

	default:
		return fmt.Errorf("dlq needs a subcommand: list|redrive")
	}
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
