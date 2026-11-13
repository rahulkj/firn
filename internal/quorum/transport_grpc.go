package quorum

// gRPC transport for the metadata quorum (ADR-007 phase B wiring). Quorum
// traffic rides the same gRPC listener as the client surface: each node's
// SlipwayService listener also registers QuorumService (see slipwayd);
// peers dial it directly. Connection setup per message is intentional — at
// 3–N members and 60ms intervals the overhead is negligible and the failure
// model is simple.

import (
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/rahulkj/slipway/internal/api/slipwaypb"
)

// TransportGRPC delivers quorum messages over gRPC.
type TransportGRPC struct {
	id    string
	peers map[string]string
}

// NewTransportGRPC creates a gRPC transport with the given peer map
// (id -> gRPC address). It must include this node.
func NewTransportGRPC(id string, peers map[string]string) (*TransportGRPC, error) {
	if id == "" {
		return nil, fmt.Errorf("slipway/quorum: id is required")
	}
	if _, ok := peers[id]; !ok {
		return nil, fmt.Errorf("slipway/quorum: peers must include self (%s)", id)
	}
	return &TransportGRPC{id: id, peers: peers}, nil
}

// Addr returns this node's advertised address.
func (t *TransportGRPC) Addr() string { return t.peers[t.id] }

// Send dials the peer and pushes one message. Short-lived connections keep
// the failure model uniform with LocalTransport (which is what production
// shares with the tests).
func (t *TransportGRPC) Send(to string, msg Message) error {
	addr, ok := t.peers[to]
	if !ok {
		return fmt.Errorf("slipway/quorum: unknown peer %q", to)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("slipway/quorum: dial %s: %w", addr, err)
	}
	defer func() { _ = conn.Close() }()
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = slipwaypb.NewQuorumServiceClient(conn).QuorumMessage(context.Background(), &slipwaypb.QuorumMessageRequest{Payload: data})
	return err
}
