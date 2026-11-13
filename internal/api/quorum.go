package api

// Quorum transport service: the wire endpoint for quorum
// messages over the client's gRPC listener. Registered in slipwayd only
// when cluster mode is enabled.

import (
	"context"
	"encoding/json"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rahulkj/slipway/internal/api/slipwaypb"
	"github.com/rahulkj/slipway/internal/quorum"
)

// QuorumServer exposes quorum message delivery as a gRPC service.
type QuorumServer struct {
	slipwaypb.UnimplementedQuorumServiceServer
	node *quorum.Node
}

// NewQuorumServer wires a quorum node into the gRPC listener.
func NewQuorumServer(node *quorum.Node) *QuorumServer { return &QuorumServer{node: node} }

// QuorumMessage delivers one quorum message to the node's mailbox.
func (s *QuorumServer) QuorumMessage(ctx context.Context, req *slipwaypb.QuorumMessageRequest) (*slipwaypb.QuorumMessageResponse, error) {
	var msg quorum.Message
	if err := json.Unmarshal(req.Payload, &msg); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid quorum message payload")
	}
	s.node.Deliver(msg)
	return &slipwaypb.QuorumMessageResponse{}, nil
}
