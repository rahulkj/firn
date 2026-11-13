package api

// ReplicaService (ADR-007 phase C): internal broker-broker replication.
// Registered on the same gRPC server as FirnService; never exposed
// outside the cluster's mTLS mesh.
//
// ReplicaPut accepts raw frames from a lease holder, validates them
// (ParseFrames CRC checks), and appends only those at or ahead of this
// node's high watermark. Out-of-order pushes (a gap ahead of HWM) are
// rejected — never silently skip.

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rahulkj/firn/internal/api/firnpb"
	"github.com/rahulkj/firn/internal/broker"
	firnlog "github.com/rahulkj/firn/internal/log"
)

const maxReplicaFramesBytes = 80 << 20 // 80 MiB

// ReplicaServer implements firnpb.ReplicaServiceServer.
type ReplicaServer struct {
	firnpb.UnimplementedReplicaServiceServer
	b *broker.Broker
}

// NewReplicaServer wires the replica service to a broker.
func NewReplicaServer(b *broker.Broker) *ReplicaServer { return &ReplicaServer{b: b} }

// internalLog returns the broker-owned internal log handle for an internal topic.
func (s *ReplicaServer) internalLog(name string) (*firnlog.FileLog, error) {
	fl, ok := s.b.InternalLog(name)
	if !ok {
		return nil, status.Error(codes.NotFound, "internal log not configured")
	}
	return fl, nil
}

// ReplicaFetch returns the raw frames in [first_offset, last_offset] —
// exactly as stored on disk (no re-encoding), preserving block identity.
func (s *ReplicaServer) ReplicaFetch(ctx context.Context, req *firnpb.ReplicaFetchRequest) (*firnpb.ReplicaFetchResponse, error) {
	if req.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic must not be empty")
	}
	if req.FirstOffset > req.LastOffset {
		return nil, status.Error(codes.InvalidArgument, "first_offset must be <= last_offset")
	}
	if isInternalTopic(req.Topic) {
		fl, err := s.internalLog(req.Topic)
		if err != nil {
			return nil, err
		}
		raw, err := fl.ReadRange(req.FirstOffset, req.LastOffset)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		if len(raw) > maxReplicaFramesBytes {
			return nil, status.Error(codes.ResourceExhausted, "replica range exceeds 80 MiB")
		}
		return &firnpb.ReplicaFetchResponse{Frames: raw}, nil
	}
	raw, err := s.b.ReadRange(ctx, req.Topic, req.Partition, req.FirstOffset, req.LastOffset)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if len(raw) > maxReplicaFramesBytes {
		return nil, status.Error(codes.ResourceExhausted, "replica range exceeds 80 MiB")
	}
	return &firnpb.ReplicaFetchResponse{Frames: raw}, nil
}

// ReplicaPut appends frames at or after this node's high watermark.
func (s *ReplicaServer) ReplicaPut(ctx context.Context, req *firnpb.ReplicaPutRequest) (*firnpb.ReplicaPutResponse, error) {
	if req.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic must not be empty")
	}
	if len(req.Frames) > maxReplicaFramesBytes {
		return nil, status.Error(codes.ResourceExhausted, "frame payload exceeds 80 MiB")
	}
	frames, err := firnlog.ParseFrames(req.Frames)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid frames: "+err.Error())
	}
	if len(frames) > 0 && frames[0].Offset != req.BaseOffset {
		return nil, status.Errorf(codes.InvalidArgument, "base_offset %d does not match first frame offset %d", req.BaseOffset, frames[0].Offset)
	}
	if isInternalTopic(req.Topic) {
		fl, err := s.internalLog(req.Topic)
		if err != nil {
			return nil, err
		}
		same, err := appendInternalFrames(fl, frames)
		if err != nil {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		return &firnpb.ReplicaPutResponse{HighWatermark: same}, nil
	}
	hwm, err := s.b.AppendReplicatedFrames(req.Topic, req.Partition, frames)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &firnpb.ReplicaPutResponse{HighWatermark: hwm}, nil
}

func isInternalTopic(name string) bool {
	for _, t := range []string{"__consumer_offsets", "__queue_attempts", "__audit", "__api_keys", "__federation_peers", "__federation_cursors"} {
		if name == t {
			return true
		}
	}
	return false
}

// appendInternalFrames applies frames to the given FileLog at or above its HWM.
func appendInternalFrames(fl *firnlog.FileLog, frames []firnlog.Frame) (int64, error) {
	hwm := fl.HighWatermark()
	for _, f := range frames {
		if f.Offset < hwm {
			continue
		}
		if f.Offset != hwm {
			return 0, status.Errorf(codes.FailedPrecondition, "replica fork: frame offset %d, local high watermark %d", f.Offset, hwm)
		}
		hwm++
		fl.AppendReplicated(f.Ts, f.Key, f.Value)
	}
	return fl.HighWatermark(), nil
}

// ReplicaServiceServer is exposed for registration in tests and harnesses.
var _ = firnpb.ReplicaServiceServer(&ReplicaServer{})
