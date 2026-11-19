package integration

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// MongoChangeStreamConnector captures change data from one MongoDB
// collection using change streams (Mongo 4.0+, replica set required —
// single-node with --replSet or a replica set). The checkpoint position is
// the change stream resume token, base64-encoded. Records are emitted per
// change event:
//
//	{"db":"...","collection":"...","op":"insert|update|replace|delete", "lsn":"...",
//	 "doc":{...}, "full_document":{...} }
//
// Position: base64 of the resume token. Absent position starts from the
// current position (change streams cannot replay history before the
// oplog window).
type MongoChangeStreamConnector struct {
	name    string
	uriEnv  string
	database string
	collection string
	fullDocument string // whenAvailable | updateLookup
	client  *mongo.Client
}

// NewMongoChangeStreamConnector configures the connector. uriEnv is the env
// var holding the Mongo connection URI. fullDocument is "whenAvailable" or
// "updateLookup" (updateLookup fetches the document as of the change, i.e.
// post-image; default whenAvailable).
func NewMongoChangeStreamConnector(name, uriEnv, database, collection, fullDocument string) (*MongoChangeStreamConnector, error) {
	if !ValidName(name) {
		return nil, fmt.Errorf("integration: invalid connector name %q", name)
	}
	if uriEnv == "" {
		return nil, errors.New("integration: uri_env required (env var holding the Mongo URI)")
	}
	if database == "" || collection == "" {
		return nil, errors.New("integration: database and collection required")
	}
	if fullDocument == "" {
		fullDocument = "whenAvailable"
	}
	if fullDocument != "whenAvailable" && fullDocument != "updateLookup" {
		return nil, fmt.Errorf("integration: full_document must be whenAvailable|updateLookup (got %q)", fullDocument)
	}
	return &MongoChangeStreamConnector{name: name, uriEnv: uriEnv, database: database, collection: collection, fullDocument: fullDocument}, nil
}

func (m *MongoChangeStreamConnector) Name() string { return m.name }

func (m *MongoChangeStreamConnector) Close() error {
	if m.client != nil {
		_ = m.client.Disconnect(context.Background())
		m.client = nil
	}
	return nil
}

func (m *MongoChangeStreamConnector) ensureClient(ctx context.Context) error {
	if m.client != nil {
		return nil
	}
	uri := os.Getenv(m.uriEnv)
	if uri == "" {
		return fmt.Errorf("integration: env %s is empty (Mongo URI)", m.uriEnv)
	}
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		return fmt.Errorf("integration: mongo connect: %w", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("integration: mongo ping: %w", err)
	}
	m.client = client
	return nil
}

// FetchBatch reads up to max change events. position is base64(resume token).
func (m *MongoChangeStreamConnector) FetchBatch(ctx context.Context, position string, max int) ([]Record, error) {
	if max <= 0 {
		max = 100
	}
	if err := m.ensureClient(ctx); err != nil {
		return nil, err
	}
	watchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	opts := options.ChangeStream().SetFullDocument(fullDocumentOption(m.fullDocument))
	if position != "" {
		raw, err := base64.StdEncoding.DecodeString(position)
		if err != nil {
			return nil, fmt.Errorf("integration: bad resume token checkpoint: %w", err)
		}
		opts.SetResumeAfter(bson.Raw(raw))
	}
	coll := m.client.Database(m.database).Collection(m.collection)
	stream, err := coll.Watch(watchCtx, mongo.Pipeline{}, opts)
	if err != nil {
		return nil, fmt.Errorf("integration: mongo watch: %w", err)
	}
	defer func() { _ = stream.Close(context.Background()) }()

	var recs []Record
	for stream.Next(watchCtx) && len(recs) < max {
		var event bson.Raw
		if err := stream.Decode(&event); err != nil {
			return nil, fmt.Errorf("integration: decode change event: %w", err)
		}
		r, err := changeEventToRecord(m, event)
		if err != nil {
			return nil, err
		}
		recs = append(recs, r)
		_ = event
	}
	if err := stream.Err(); err != nil && watchCtx.Err() == nil {
		return nil, fmt.Errorf("integration: change stream error: %w", err)
	}
	if len(recs) > 0 {
		return recs, nil
	}
	return nil, nil
}

func fullDocumentOption(s string) options.FullDocument {
	switch s {
	case "updateLookup":
		return options.UpdateLookup
	default:
		return options.WhenAvailable
	}
}

// changeEventToRecord marshals one change event into a Firn record.
func changeEventToRecord(m *MongoChangeStreamConnector, event bson.Raw) (Record, error) {
	doc := map[string]any{}
	if err := bson.Unmarshal(event, &doc); err != nil {
		return Record{}, fmt.Errorf("integration: unmarshal change event: %w", err)
	}
	out := map[string]any{
		"db":         m.database,
		"collection": m.collection,
		"op":         fmt.Sprint(doc["operationType"]),
	}
	if ns, ok := doc["ns"].(map[string]any); ok {
		out["ns"] = ns["db"].(string) + "." + ns["coll"].(string)
	}
	for _, k := range []string{"documentKey", "fullDocument", "updateDescription"} {
		if v, ok := doc[k]; ok {
			out[k] = v
		}
	}
	resumeToken := doc["_id"]
	ridBytes, err := bson.Marshal(resumeToken)
	if err != nil {
		return Record{}, fmt.Errorf("integration: marshal resume token: %w", err)
	}
	r := Record{
		Position: base64.StdEncoding.EncodeToString(ridBytes),
		Headers: map[string]string{
			"source_collection": m.database + "." + m.collection,
			"op":                fmt.Sprint(doc["operationType"]),
		},
	}
	v, err := json.Marshal(out)
	if err != nil {
		return Record{}, err
	}
	r.Value = v
	// Key: documentKey._id when present.
	if dk, ok := doc["documentKey"].(map[string]any); ok {
		if id, ok := dk["_id"]; ok {
			if kb, err := json.Marshal(id); err == nil {
				r.Key = kb
			}
		}
	}
	if r.Key == nil {
		r.Key = []byte(m.database + "." + m.collection)
	}
	return r, nil
}
