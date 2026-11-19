package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	dynamodbstreamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

// DynamoDBStreamsConnector captures changes from one DynamoDB Streams
// stream (identified by its stream ARN). The checkpoint position is a JSON
// object mapping shard id → last sequence number:
//
//	{"shard-0000000044123A8C":"13000000000000000623"}
//
// A fresh connector starts from the stream's oldest available records
// (TRIM_HORIZON). Records are emitted per stream record:
//
//	{"op":"INSERT|MODIFY|REMOVE","event_id":"...","event_source":"aws:dynamodb",
//	 "stream":"...","keys":{...},"new_image":{...},"old_image":{...},"lsn":"..."}
//
// lsn is the record's sequence number. Attribute values are unmarshaled to
// native JSON types (DynamoDB's typed-attribute encoding is decoded away).
type DynamoDBStreamsConnector struct {
	name           string
	streamArn      string
	region         string
	endpoint       string
	position       map[string]string
	shardIterators map[string]string
	cl             *dynamodbstreams.Client
}

// NewDynamoDBStreamsConnector configures the connector. streamArn is the
// DynamoDB Streams stream ARN (find it via: aws dynamodb describe-table
// --table-name T → LatestStreamArn). region defaults to AWS_REGION.
// endpoint is optional (e.g. "http://localhost:58000" for dynamodb-local);
// credentials come from AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY[/AWS_SESSION_TOKEN].
func NewDynamoDBStreamsConnector(name, streamArn, region, endpoint string) (*DynamoDBStreamsConnector, error) {
	if !ValidName(name) {
		return nil, fmt.Errorf("integration: invalid connector name %q", name)
	}
	if streamArn == "" {
		return nil, errors.New("integration: stream_arn required")
	}
	if region == "" {
		region = os.Getenv("AWS_REGION")
	}
	if region == "" {
		region = os.Getenv("AWS_DEFAULT_REGION")
	}
	if region == "" {
		return nil, errors.New("integration: region required (config or AWS_REGION)")
	}
	return &DynamoDBStreamsConnector{
		name:           name,
		streamArn:      streamArn,
		region:         region,
		endpoint:       endpoint,
		position:       map[string]string{},
		shardIterators: map[string]string{},
	}, nil
}

func (d *DynamoDBStreamsConnector) Name() string { return d.name }

func (d *DynamoDBStreamsConnector) Close() error { return nil }

func (d *DynamoDBStreamsConnector) client() *dynamodbstreams.Client {
	if d.cl == nil {
		opts := dynamodbstreams.Options{
			Region:      d.region,
			Credentials: credentials.NewStaticCredentialsProvider(os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), os.Getenv("AWS_SESSION_TOKEN")),
		}
		if d.endpoint != "" {
			opts.BaseEndpoint = aws.String(d.endpoint)
			// Local/testing endpoints (e.g. dynamodb-local) typically use dummy credentials.
			if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
				opts.Credentials = credentials.NewStaticCredentialsProvider("x", "x", "")
			}
		}
		d.cl = dynamodbstreams.New(opts)
	}
	return d.cl
}

// FetchBatch polls each shard at most once and accumulates up to max records.
func (d *DynamoDBStreamsConnector) FetchBatch(ctx context.Context, position string, max int) ([]Record, error) {
	if max <= 0 {
		max = 100
	}
	if err := d.loadPosition(position); err != nil {
		return nil, err
	}
	fetchCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()

	out, err := d.client().DescribeStream(fetchCtx, &dynamodbstreams.DescribeStreamInput{
		StreamArn: aws.String(d.streamArn),
	})
	if err != nil {
		return nil, fmt.Errorf("integration: dynamodb describe stream: %w", err)
	}
	shards := out.StreamDescription.Shards

	var recs []Record
	for _, shard := range shards {
		shardID := aws.ToString(shard.ShardId)
		if len(recs) >= max {
			break
		}
		shardRecs, err := d.pollShard(fetchCtx, shardID, shard, max-len(recs))
		if err != nil {
			// A shard that no longer exists or moved: drop its iterator and continue.
			delete(d.shardIterators, shardID)
			continue
		}
		recs = append(recs, shardRecs...)
	}
	return recs, nil
}

func (d *DynamoDBStreamsConnector) loadPosition(position string) error {
	if position == "" {
		return nil
	}
	var m map[string]string
	if err := json.Unmarshal([]byte(position), &m); err != nil {
		return fmt.Errorf("integration: bad stream position checkpoint: %w", err)
	}
	d.position = m
	return nil
}

func (d *DynamoDBStreamsConnector) pollShard(ctx context.Context, shardID string, shard dynamodbstreamstypes.Shard, max int) ([]Record, error) {
	iterator, ok := d.shardIterators[shardID]
	if !ok {
		var err error
		iterator, err = d.shardIterator(ctx, shardID, shard)
		if err != nil {
			return nil, err
		}
	}
	page, err := d.client().GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
		ShardIterator: aws.String(iterator),
		Limit:         aws.Int32(int32(max)),
	})
	if err != nil {
		return nil, err
	}
	if next := aws.ToString(page.NextShardIterator); next != "" {
		d.shardIterators[shardID] = next
	} else {
		delete(d.shardIterators, shardID) // shard closed
	}
	var out []Record
	for _, r := range page.Records {
		seq := ""
		if r.Dynamodb != nil {
			seq = aws.ToString(r.Dynamodb.SequenceNumber)
			d.position[shardID] = seq
		}
		rec := d.streamRecordToRecord(r, seq)
		out = append(out, rec)
		if len(out) >= max {
			break
		}
	}
	return out, nil
}

func (d *DynamoDBStreamsConnector) shardIterator(ctx context.Context, shardID string, shard dynamodbstreamstypes.Shard) (string, error) {
	lastSeq := d.position[shardID]
	seqType := dynamodbstreamstypes.ShardIteratorTypeTrimHorizon
	if lastSeq != "" {
		seqType = dynamodbstreamstypes.ShardIteratorTypeAfterSequenceNumber
	} else if shard.ParentShardId != nil {
		if parentSeq, ok := d.position[aws.ToString(shard.ParentShardId)]; ok && parentSeq != "" {
			lastSeq = parentSeq
			seqType = dynamodbstreamstypes.ShardIteratorTypeAfterSequenceNumber
		}
	}
	input := &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         aws.String(d.streamArn),
		ShardId:           aws.String(shardID),
		ShardIteratorType: seqType,
	}
	if seqType == dynamodbstreamstypes.ShardIteratorTypeAfterSequenceNumber {
		input.SequenceNumber = aws.String(lastSeq)
	}
	out, err := d.client().GetShardIterator(ctx, input)
	if err != nil {
		return "", fmt.Errorf("integration: get shard iterator for %s: %w", shardID, err)
	}
	return aws.ToString(out.ShardIterator), nil
}

func (d *DynamoDBStreamsConnector) streamRecordToRecord(r dynamodbstreamstypes.Record, seq string) Record {
	eventID := aws.ToString(r.EventID)
	out := map[string]any{
		"op":           string(r.EventName),
		"event_id":     eventID,
		"event_source": aws.ToString(r.EventSource),
		"stream":       d.streamArn,
		"lsn":          seq,
	}
	if r.Dynamodb != nil {
		if img := imageToMap(r.Dynamodb.Keys); img != nil {
			out["keys"] = img
		}
		if img := imageToMap(r.Dynamodb.NewImage); img != nil {
			out["new_image"] = img
		}
		if img := imageToMap(r.Dynamodb.OldImage); img != nil {
			out["old_image"] = img
		}
	}
	v, err := json.Marshal(out)
	if err != nil {
		return Record{}
	}
	posBytes, err := json.Marshal(d.position)
	if err != nil {
		return Record{}
	}
	r2 := Record{
		Value:    v,
		Position: string(posBytes),
		Headers: map[string]string{
			"op":     string(r.EventName),
			"lsn":    seq,
			"stream": d.streamArn,
		},
	}
	if img := imageToMap(r.Dynamodb.Keys); img != nil {
		kb, err := json.Marshal(img)
		if err == nil {
			r2.Key = kb
		}
	}
	if r2.Key == nil {
		r2.Key = []byte(fmt.Sprintf("%s:%s", r.EventName, eventID))
	}
	return r2
}

func imageToMap(img map[string]dynamodbstreamstypes.AttributeValue) map[string]any {
	if img == nil {
		return nil
	}
	// The streams SDK's AttributeValue is not the same concrete type as the
	// dynamodb service package's, so decode through JSON. Output keeps the
	// raw typed shape ({"S":"..."} / {"N":"..."}); document as typed_raw.
	b, err := json.Marshal(img)
	if err != nil {
		return nil
	}
	m := map[string]any{}
	if err := json.Unmarshal(b, &m); err != nil {
		return nil
	}
	return m
}
