// RoutedBrokerServer implements the mesh-only RoutedBrokerService.
// Serves one internal call today: delivery of a record produced by the
// DLQ diverter to the node holding the partition lease. Registered on the
// mesh listener only; the public listener never sees this service.
package api

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rahulkj/firn/internal/api/firnpb"
	"github.com/rahulkj/firn/internal/broker"
)

type RoutedBrokerServer struct {
	firnpb.UnimplementedRoutedBrokerServiceServer
	b *broker.Broker
}

func NewRoutedBrokerServer(b *broker.Broker) *RoutedBrokerServer {
	return &RoutedBrokerServer{b: b}
}

// RoutedProduce is called by a peer's DLQ diverter to land one record on
// this node (which holds the partition lease, or close enough).
func (s *RoutedBrokerServer) RoutedProduce(ctx context.Context, req *firnpb.RoutedProduceRequest) (*firnpb.RoutedProduceResponse, error) {
	if req.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic must not be empty")
	}
	p, off, err := s.b.Produce(ctx, req.Topic, req.Key, req.Value)
	if err != nil {
		return nil, toStatus(err)
	}
	return &firnpb.RoutedProduceResponse{Partition: p, Offset: off}, nil
}
