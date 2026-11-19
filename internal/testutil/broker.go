// Package testutil provides shared fixtures for integration tests across
// this module (in-process brokers over loopback TCP). Not used in production.
package testutil

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/rahulkj/firn/internal/api"
	"github.com/rahulkj/firn/internal/api/firnpb"
	"github.com/rahulkj/firn/internal/broker"
	"github.com/rahulkj/firn/internal/meta"
	"github.com/rahulkj/firn/internal/queue"
)

// TopicSpec describes a topic to create: partition count and mode.
type TopicSpec struct {
	Partitions int
	Mode       string // "stream" (default) or "queue"
}

// StartBroker creates an in-process single-node broker with the given
// topics (name -> partitions, stream mode) and returns its client gRPC address.
func StartBroker(t *testing.T, topics map[string]int) string {
	t.Helper()
	specs := map[string]TopicSpec{}
	for name, partitions := range topics {
		specs[name] = TopicSpec{Partitions: partitions, Mode: meta.ModeStream}
	}
	return StartBrokerWithModes(t, specs)
}

// StartBrokerWithModes is StartBroker with per-topic mode control.
func StartBrokerWithModes(t *testing.T, topics map[string]TopicSpec) string {
	t.Helper()
	reg := meta.NewRegistry()
	for name, spec := range topics {
		if err := reg.CreateWithMode(name, spec.Partitions, spec.Mode); err != nil {
			t.Fatalf("create topic %q: %v", name, err)
		}
	}
	dataDir := t.TempDir()
	b, err := broker.NewWithDir(reg, dataDir, broker.WithMaxSegmentBytes(1<<20))
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	var qm *queue.Manager
	for _, topic := range reg.List() {
		if topic.Mode == meta.ModeQueue {
			q, err := queue.NewManager(reg, b, dataDir)
			if err != nil {
				t.Fatalf("queue manager: %v", err)
			}
			qm = q
			break
		}
	}
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	firnpb.RegisterFirnServiceServer(srv, api.NewServer(b, qm))
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = ln.Close()
	})
	deadline := time.Now().Add(2 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", ln.Addr().String(), 50*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("broker did not come up: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return ln.Addr().String()
}
