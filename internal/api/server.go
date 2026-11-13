// Package api exposes firn over the network: gRPC + HTTP admin/health.
//
// Design decision (docs/adr/002-api.md): a clean, minimal API first.
// Wire compatibility with any other broker is a non-goal for v1; if it ever
// arrives, it ships as a separate protocol gateway, never inside the broker core.
package api

import (
	"context"
	"errors"
	"log"
	"sort"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rahulkj/firn/internal/api/firnpb"
	"github.com/rahulkj/firn/internal/broker"
	"github.com/rahulkj/firn/internal/fence"
	"github.com/rahulkj/firn/internal/groups"
	firnlog "github.com/rahulkj/firn/internal/log"
	"github.com/rahulkj/firn/internal/meta"
	"github.com/rahulkj/firn/internal/metrics"
	"github.com/rahulkj/firn/internal/queue"
)

// Server implements firnpb.FirnServiceServer.
type Server struct {
	firnpb.UnimplementedFirnServiceServer
	b   *broker.Broker
	q   *queue.Manager
	met *metrics.Metrics
}

// NewServer returns a Server orchestrating b and q. q may be nil; then the
// queue RPCs return Unimplemented. met may be nil; then metrics are not
// recorded.
func NewServer(b *broker.Broker, q *queue.Manager, met ...*metrics.Metrics) *Server {
	var m *metrics.Metrics
	if len(met) > 0 {
		m = met[0]
	}
	return &Server{b: b, q: q, met: m}
}

// Produce routes req to a partition (FNV(key) % partitions, or round-robin
// for empty keys) and appends it to the partition log.
func (s *Server) Produce(ctx context.Context, req *firnpb.ProduceRequest) (*firnpb.ProduceResponse, error) {
	if req.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic must not be empty")
	}
	partition, offset, err := s.b.Produce(ctx, req.Topic, req.Key, req.Value)
	if err != nil {
		return nil, toStatus(err)
	}
	if s.met != nil {
		s.met.ProduceTotal.WithLabelValues(req.Topic).Inc()
	}
	return &firnpb.ProduceResponse{Partition: partition, Offset: offset}, nil
}

// Fetch returns up to max_records records from (topic, partition) starting
// at offset. A fetch at the high watermark returns an empty response.
func (s *Server) Fetch(ctx context.Context, req *firnpb.FetchRequest) (*firnpb.FetchResponse, error) {
	if req.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic must not be empty")
	}
	if req.Offset < 0 {
		return nil, status.Error(codes.InvalidArgument, "offset must be >= 0")
	}
	if req.MaxRecords < 1 {
		return nil, status.Error(codes.InvalidArgument, "max_records must be >= 1")
	}
	if s.met != nil {
		s.met.FetchTotal.WithLabelValues(req.Topic).Inc()
	}
	recs, err := s.b.Fetch(ctx, req.Topic, req.Partition, req.Offset, int(req.MaxRecords))
	if err != nil {
		return nil, toStatus(err)
	}
	out := &firnpb.FetchResponse{Records: make([]*firnpb.Record, 0, len(recs))}
	for _, r := range recs {
		out.Records = append(out.Records, &firnpb.Record{Offset: r.Offset, Key: r.Key, Value: r.Value})
	}
	return out, nil
}

func toAssignment(in map[string][]int32) []*firnpb.TopicAssignment {
	topicSet := make([]string, 0, len(in))
	for topic := range in {
		topicSet = append(topicSet, topic)
	}
	sort.Strings(topicSet)
	out := make([]*firnpb.TopicAssignment, 0, len(topicSet))
	for _, topic := range topicSet {
		out = append(out, &firnpb.TopicAssignment{Topic: topic, Partitions: append([]int32(nil), in[topic]...)})
	}
	return out
}

func groupToStatus(err error) error {
	switch {
	case errors.Is(err, groups.ErrUnknownTopic), errors.Is(err, groups.ErrGroupNotFound), errors.Is(err, groups.ErrMemberNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, groups.ErrInvalidRequest):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, groups.ErrStaleGeneration), errors.Is(err, groups.ErrOffsetRegression):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return toStatus(err)
	}
}

// JoinGroup adds the member to the group and returns its assignment.
func (s *Server) JoinGroup(ctx context.Context, req *firnpb.JoinGroupRequest) (*firnpb.JoinGroupResponse, error) {
	if req.GroupId == "" {
		return nil, status.Error(codes.InvalidArgument, "group_id must not be empty")
	}
	res, err := s.b.Groups().JoinGroup(req.GroupId, req.MemberId, req.Topics, req.Protocol, time.Duration(req.SessionTimeoutMs)*time.Millisecond)
	if err != nil {
		return nil, groupToStatus(err)
	}
	return &firnpb.JoinGroupResponse{
		MemberId:     res.MemberID,
		GenerationId: res.Generation,
		Leader:       res.Leader,
		Assignment:   toAssignment(res.Assignment),
		Members:      res.Members,
		Protocol:     res.Protocol,
	}, nil
}

// SyncGroup returns the member's current assignment.
func (s *Server) SyncGroup(ctx context.Context, req *firnpb.SyncGroupRequest) (*firnpb.SyncGroupResponse, error) {
	if req.GroupId == "" || req.MemberId == "" {
		return nil, status.Error(codes.InvalidArgument, "group_id and member_id are required")
	}
	assignment, generation, err := s.b.Groups().SyncGroup(req.GroupId, req.MemberId, req.GenerationId)
	if err != nil {
		return nil, groupToStatus(err)
	}
	return &firnpb.SyncGroupResponse{Assignment: toAssignment(assignment), GenerationId: generation}, nil
}

// Heartbeat refreshes the member lease and reports stale generations.
func (s *Server) Heartbeat(ctx context.Context, req *firnpb.HeartbeatRequest) (*firnpb.HeartbeatResponse, error) {
	if req.GroupId == "" || req.MemberId == "" {
		return nil, status.Error(codes.InvalidArgument, "group_id and member_id are required")
	}
	generation, rebalance, err := s.b.Groups().Heartbeat(req.GroupId, req.MemberId, req.GenerationId)
	if err != nil {
		return nil, groupToStatus(err)
	}
	return &firnpb.HeartbeatResponse{GenerationId: generation, RebalanceRequired: rebalance}, nil
}

// LeaveGroup removes the member and rebalances.
func (s *Server) LeaveGroup(ctx context.Context, req *firnpb.LeaveGroupRequest) (*firnpb.LeaveGroupResponse, error) {
	if req.GroupId == "" || req.MemberId == "" {
		return nil, status.Error(codes.InvalidArgument, "group_id and member_id are required")
	}
	if err := s.b.Groups().LeaveGroup(req.GroupId, req.MemberId); err != nil {
		return nil, groupToStatus(err)
	}
	return &firnpb.LeaveGroupResponse{}, nil
}

// CommitOffset commits an offset for (group, topic, partition).
func (s *Server) CommitOffset(ctx context.Context, req *firnpb.CommitOffsetRequest) (*firnpb.CommitOffsetResponse, error) {
	if req.GroupId == "" || req.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "group_id and topic are required")
	}
	if req.Partition < 0 || req.Offset < 0 {
		return nil, status.Error(codes.InvalidArgument, "partition and offset must be >= 0")
	}
	if err := s.b.Groups().CommitOffset(req.GroupId, req.Topic, req.Partition, req.Offset, req.GenerationId, req.AllowReset); err != nil {
		return nil, groupToStatus(err)
	}
	return &firnpb.CommitOffsetResponse{}, nil
}

// FetchOffset returns the folded committed offset for a partition.
func (s *Server) FetchOffset(ctx context.Context, req *firnpb.FetchOffsetRequest) (*firnpb.FetchOffsetResponse, error) {
	if req.GroupId == "" || req.Topic == "" || req.Partition < 0 {
		return nil, status.Error(codes.InvalidArgument, "group_id, topic, and partition >= 0 are required")
	}
	v, found, err := s.b.Groups().FetchOffset(req.GroupId, req.Topic, req.Partition)
	if err != nil {
		return nil, groupToStatus(err)
	}
	return &firnpb.FetchOffsetResponse{Offset: v.Offset, Found: found, GenerationId: v.Generation, Leader: v.Leader}, nil
}

// FetchGroupOffsets lists the folded committed offsets for a group.
func (s *Server) FetchGroupOffsets(ctx context.Context, req *firnpb.FetchGroupOffsetsRequest) (*firnpb.FetchGroupOffsetsResponse, error) {
	if req.GroupId == "" {
		return nil, status.Error(codes.InvalidArgument, "group_id must not be empty")
	}
	entries, err := s.b.Groups().FetchGroupOffsets(req.GroupId)
	if err != nil {
		return nil, groupToStatus(err)
	}
	out := &firnpb.FetchGroupOffsetsResponse{Offsets: make([]*firnpb.OffsetEntry, 0, len(entries))}
	for _, e := range entries {
		out.Offsets = append(out.Offsets, &firnpb.OffsetEntry{Topic: e.Topic, Partition: e.Partition, Offset: e.Offset, GenerationId: e.Generation, Leader: e.Leader})
	}
	return out, nil
}

func queueToStatus(err error) error {
	switch {
	case errors.Is(err, queue.ErrQueueNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, queue.ErrNotAQueue), errors.Is(err, queue.ErrInvalidRequest):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, queue.ErrUnknownDelivery):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, queue.ErrFederatedPassive):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return toStatus(err)
	}
}

// Pull claims up to max_messages unacked messages from a queue.
func (s *Server) Pull(ctx context.Context, req *firnpb.PullRequest) (*firnpb.PullResponse, error) {
	if s.q == nil {
		return nil, status.Error(codes.Unimplemented, "queue manager not configured")
	}
	if req.Queue == "" {
		return nil, status.Error(codes.InvalidArgument, "queue must not be empty")
	}
	ds, err := s.q.Pull(ctx, req.Queue, int(req.MaxMessages), time.Duration(req.VisibilityTimeoutMs)*time.Millisecond)
	if err != nil {
		return nil, queueToStatus(err)
	}
	out := &firnpb.PullResponse{Deliveries: make([]*firnpb.QueueDelivery, 0, len(ds))}
	for _, d := range ds {
		out.Deliveries = append(out.Deliveries, &firnpb.QueueDelivery{
			DeliveryId: d.DeliveryID,
			Queue:      d.Queue,
			Partition:  d.Partition,
			Offset:     d.Offset,
			Key:        d.Key,
			Value:      d.Value,
			Attempts:   d.Attempts,
			DeadlineMs: d.DeadlineMs,
		})
	}
	return out, nil
}

// Ack marks a delivered message as consumed.
func (s *Server) Ack(ctx context.Context, req *firnpb.AckRequest) (*firnpb.AckResponse, error) {
	if s.q == nil {
		return nil, status.Error(codes.Unimplemented, "queue manager not configured")
	}
	if req.Queue == "" || req.DeliveryId == "" {
		return nil, status.Error(codes.InvalidArgument, "queue and delivery_id are required")
	}
	if err := s.q.Ack(ctx, req.Queue, req.DeliveryId); err != nil {
		return nil, queueToStatus(err)
	}
	return &firnpb.AckResponse{}, nil
}

// GetMetadata returns topic/partition metadata and the advertised node
// list. In cluster mode it also reports each partition's lease holder.
func (s *Server) GetMetadata(ctx context.Context, req *firnpb.GetMetadataRequest) (*firnpb.GetMetadataResponse, error) {
	out := &firnpb.GetMetadataResponse{}

	view := s.b.ClusterView()
	if view == nil {
		out.Brokers = []*firnpb.BrokerMetadata{{Id: "single", Addr: "localhost:9093", IsLeader: true}}
	} else {
		leader := view.Leader()
		for _, id := range view.AllNodes() {
			addr, ok := view.NodeAddr(id)
			if !ok {
				addr = ""
			}
			out.Brokers = append(out.Brokers, &firnpb.BrokerMetadata{Id: id, Addr: addr, IsLeader: id == leader})
		}
	}

	// Which topics to include?
	names := []string{}
	if len(req.Topics) == 0 {
		for _, t := range s.b.ListTopics() {
			names = append(names, t.Name)
		}
	} else {
		names = req.Topics
	}

	for _, name := range names {
		topic, err := s.b.TopicMeta(name)
		if err != nil {
			continue // topic may be absent; skip silently
		}
		m := &firnpb.TopicMetadata{Name: topic.Name, Partitions: int32(topic.Partitions), Mode: topic.Mode}
		for p := 0; p < topic.Partitions; p++ {
			var holderID, holderAddr string
			if a := s.b.Advisor(); a != nil {
				holderID = a.Holder(topic.Name, int32(p))
			}
			if view != nil && holderID != "" {
				if addr, ok := view.NodeAddr(holderID); ok {
					holderAddr = addr
				}
			}
			m.PartitionsMeta = append(m.PartitionsMeta, &firnpb.PartitionMetadata{
				PartitionId: int32(p),
				Leader:      holderID,
				LeaderAddr:  holderAddr,
			})
		}
		out.Topics = append(out.Topics, m)
	}
	return out, nil
}

func toStatus(err error) error {
	switch {
	case errors.Is(err, meta.ErrTopicNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, broker.ErrPartitionOutOfRange), errors.Is(err, broker.ErrInvalidRequest):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, firnlog.ErrOffsetOutOfRange), errors.Is(err, fence.ErrFenced), errors.Is(err, broker.ErrFederatedPassive):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		log.Printf("firn/api: internal error: %v", err)
		return status.Error(codes.Internal, "internal error")
	}
}
