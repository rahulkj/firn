package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Live MongoDB change-stream test. Requires MONGO_TEST_URI pointing at a
// replica set, e.g.
//   MONGO_TEST_URI='mongodb://127.0.0.1:57017/?replicaSet=rs0' go test ./internal/integration/ -run TestMongoChangeStream -v
// Skips silently when unset.
func TestMongoChangeStreamLive(t *testing.T) {
	uri := envOrSkip(t, "MONGO_TEST_URI")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	coll := client.Database("integration_test").Collection("events")
	if _, err := coll.DeleteMany(ctx, map[string]any{}); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	c, err := NewMongoChangeStreamConnector("mongo-test", "MONGO_TEST_URI", "integration_test", "events", "updateLookup")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	// Insert AFTER the stream exists (change streams don't replay history).
	go func() {
		time.Sleep(500 * time.Millisecond)
		for i := 0; i < 3; i++ {
			_, _ = coll.InsertOne(ctx, map[string]any{"note": fmt.Sprintf("n%d", i), "qty": i})
		}
	}()

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
			if r.Position == "" {
				t.Fatal("record without position")
			}
			position = r.Position
		}
		if seen["insert"] >= 3 {
			break
		}
		if len(recs) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if seen["insert"] != 3 {
		t.Fatalf("inserts seen = %d, want 3 (%v)", seen["insert"], seen)
	}

	// Update one doc; expect one update event with the post-image.
	go func() {
		time.Sleep(500 * time.Millisecond)
		_, _ = coll.UpdateOne(ctx, map[string]any{"note": "n0"}, map[string]any{"$set": map[string]any{"qty": 100}})
	}()
	foundUpdate := false
	for range 60 {
		recs, err := c.FetchBatch(ctx, position, 10)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			var m map[string]any
			_ = json.Unmarshal(r.Value, &m)
			if fmt.Sprint(m["op"]) == "update" {
				foundUpdate = true
			}
			position = r.Position
		}
		if foundUpdate {
			break
		}
		if len(recs) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if !foundUpdate {
		t.Fatal("update event not observed")
	}

	// Delete; expect a delete event.
	go func() {
		time.Sleep(500 * time.Millisecond)
		_, _ = coll.DeleteOne(ctx, map[string]any{"note": "n1"})
	}()
	foundDelete := false
	for range 60 {
		recs, err := c.FetchBatch(ctx, position, 10)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			var m map[string]any
			_ = json.Unmarshal(r.Value, &m)
			if fmt.Sprint(m["op"]) == "delete" {
				foundDelete = true
			}
			position = r.Position
		}
		if foundDelete {
			break
		}
		if len(recs) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if !foundDelete {
		t.Fatal("delete event not observed")
	}
}

func TestMongoConnectorValidation(t *testing.T) {
	if _, err := NewMongoChangeStreamConnector("bad name!", "MONGO_TEST_URI", "db", "coll", ""); err == nil {
		t.Fatal("bad name accepted")
	}
	if _, err := NewMongoChangeStreamConnector("ok", "", "db", "coll", ""); err == nil {
		t.Fatal("missing uri_env accepted")
	}
	if _, err := NewMongoChangeStreamConnector("ok", "MONGO_TEST_URI", "", "coll", ""); err == nil {
		t.Fatal("missing database accepted")
	}
	if _, err := NewMongoChangeStreamConnector("ok", "MONGO_TEST_URI", "db", "coll", "bogus"); err == nil {
		t.Fatal("bad full_document accepted")
	}
}
