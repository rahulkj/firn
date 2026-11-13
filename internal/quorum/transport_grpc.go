package quorum

// gRPC transport for the metadata quorum (ADR-007 phases B–C wiring).
// Quorum traffic rides the mesh gRPC listener — a SEPARATE listener from the
// client-facing FirnService port (see firnd) — behind mTLS with a
// cluster-wide mesh CA. Peers authenticate with node certificates, not API
// keys. Connection setup per message is intentional — at N members and 60 ms
// intervals the overhead is negligible and the failure model is simple.

import (
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/rahulkj/firn/internal/api/firnpb"
	"github.com/rahulkj/firn/internal/meshca"
)

// TransportGRPC delivers quorum messages over gRPC with mesh mTLS.
type TransportGRPC struct {
	id    string
	peers map[string]string
	mesh  *meshca.Bundle
}

// NewTransportGRPC creates a gRPC transport with the given peer map
// (id -> mesh gRPC address). It must include this node.
func NewTransportGRPC(id string, peers map[string]string, mesh *meshca.Bundle) (*TransportGRPC, error) {
	if id == "" {
		return nil, fmt.Errorf("firn/quorum: id is required")
	}
	if mesh == nil {
		return nil, fmt.Errorf("firn/quorum: mesh bundle is required (mesh mTLS is mandatory)")
	}
	if _, ok := peers[id]; !ok {
		return nil, fmt.Errorf("firn/quorum: peers must include self (%s)", id)
	}
	return &TransportGRPC{id: id, peers: peers, mesh: mesh}, nil
}

// Addr returns this node's advertised address (mesh listener).
func (t *TransportGRPC) Addr() string { return t.peers[t.id] }

// Send dials the peer and pushes one message. Short-lived connections keep
// the failure model uniform with LocalTransport (which is what production
// shares with the tests).
func (t *TransportGRPC) Send(to string, msg Message) error {
	addr, ok := t.peers[to]
	if !ok {
		return fmt.Errorf("firn/quorum: unknown peer %q", to)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(t.mesh.ClientTLSConfig(to))))
	if err != nil {
		return fmt.Errorf("firn/quorum: dial %s: %w", addr, err)
	}
	defer func() { _ = conn.Close() }()
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = firnpb.NewQuorumServiceClient(conn).QuorumMessage(context.Background(), &firnpb.QuorumMessageRequest{Payload: data})
	return err
}
