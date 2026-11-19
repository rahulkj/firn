package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Live DynamoDB Streams test. Requires DDB_TEST_ENDPOINT, e.g.
//   DDB_TEST_ENDPOINT=http://127.0.0.1:58000 go test ./internal/integration/ -run TestDynamoDBStreams -v
// Skips silently when unset.
func TestDynamoDBStreamsLive(t *testing.T) {
	endpoint := envOrSkip(t, "DDB_TEST_ENDPOINT")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg := aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("x", "x", "")}
	ddb := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = aws.String(endpoint) })

	table := fmt.Sprintf("integration-test-%d", time.Now().UnixNano())
	_, err := ddb.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(table),
		KeySchema: []types.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash}},
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS}},
		BillingMode:          types.BillingModePayPerRequest,
		StreamSpecification:  &types.StreamSpecification{StreamEnabled: aws.Bool(true), StreamViewType: types.StreamViewTypeNewAndOldImages},
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		out, err := ddb.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(table)})
		if err != nil {
			t.Fatal(err)
		}
		if out.Table.TableStatus == types.TableStatusActive {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	out, err := ddb.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(table)})
	if err != nil {
		t.Fatal(err)
	}
	streamArn := aws.ToString(out.Table.LatestStreamArn)

	c, err := NewDynamoDBStreamsConnector("ddb-test", streamArn, "us-east-1", endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	for i := 0; i < 3; i++ {
		_, err := ddb.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(table),
			Item:      map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: fmt.Sprintf("k%d", i)}, "qty": &types.AttributeValueMemberN{Value: fmt.Sprint(i * 10)}},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	position := ""
	seen := map[string]int{}
	for range 60 {
		recs, err := c.FetchBatch(ctx, position, 10)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			var m map[string]any
			if err := json.Unmarshal(r.Value, &m); err != nil {
				t.Fatal(err)
			}
			seen[fmt.Sprint(m["op"])]++
			position = r.Position
		}
		if seen["INSERT"] >= 3 {
			break
		}
		if len(recs) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if seen["INSERT"] != 3 {
		t.Fatalf("inserts seen = %d, want 3 (%v)", seen["INSERT"], seen)
	}

	// Modify one item.
	_, err = ddb.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item:      map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: "k0"}, "qty": &types.AttributeValueMemberN{Value: "999"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	foundModify := false
	for range 60 {
		recs, err := c.FetchBatch(ctx, position, 10)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			var m map[string]any
			_ = json.Unmarshal(r.Value, &m)
			if fmt.Sprint(m["op"]) == "MODIFY" {
				foundModify = true
			}
			position = r.Position
		}
		if foundModify {
			break
		}
		if len(recs) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if !foundModify {
		t.Fatal("MODIFY record not observed")
	}

	// Remove one item.
	_, err = ddb.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(table),
		Key:       map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: "k1"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	foundRemove := false
	for range 60 {
		recs, err := c.FetchBatch(ctx, position, 10)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			var m map[string]any
			_ = json.Unmarshal(r.Value, &m)
			if fmt.Sprint(m["op"]) == "REMOVE" {
				foundRemove = true
			}
			position = r.Position
		}
		if foundRemove {
			break
		}
		if len(recs) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if !foundRemove {
		t.Fatal("REMOVE record not observed")
	}

	// Checkpoint resume: new connector instance, from the same position,
	// expects no duplicates.
	c2, err := NewDynamoDBStreamsConnector("ddb-test-2", streamArn, "us-east-1", endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c2.Close() }()
	recs, err := c2.FetchBatch(ctx, position, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 0 {
		t.Fatalf("resumed connector saw %d records, want 0 (position not honored)", len(recs))
	}
}

func TestDynamoDBConnectorValidation(t *testing.T) {
	if _, err := NewDynamoDBStreamsConnector("bad name!", "arn:aws:dynamodb:us-east-1:1:table/t/stream/1", "us-east-1", ""); err == nil {
		t.Fatal("bad name accepted")
	}
	if _, err := NewDynamoDBStreamsConnector("ok", "", "us-east-1", ""); err == nil {
		t.Fatal("missing stream_arn accepted")
	}
}
