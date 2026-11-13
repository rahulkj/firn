package meta

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenRegistryCreatesAndPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "topics.json")

	r, err := OpenRegistry(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := r.Create("orders", 3); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := r.Create("events", 1); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Reopen: topics must survive.
	r2, err := OpenRegistry(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	topics := r2.List()
	if len(topics) != 2 || topics[0].Name != "events" || topics[0].Partitions != 1 || topics[1].Name != "orders" || topics[1].Partitions != 3 {
		t.Fatalf("topics after reopen = %+v", topics)
	}

	// The file must be valid JSON with the expected schema.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read topics file: %v", err)
	}
	var decoded struct {
		Topics []Topic `json:"topics"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("topics file is not valid JSON: %v", err)
	}
	if len(decoded.Topics) != 2 {
		t.Fatalf("topics file holds %d topics, want 2", len(decoded.Topics))
	}
}

func TestOpenRegistryDuplicateStillConflicts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "topics.json")
	r, err := OpenRegistry(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := r.Create("orders", 1); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := r.Create("orders", 2); !errors.Is(err, ErrTopicExists) {
		t.Fatalf("duplicate create err = %v, want ErrTopicExists", err)
	}
}

func TestOpenRegistryRejectsCorruptFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "topics.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := OpenRegistry(path); err == nil {
		t.Fatal("corrupt topics file: want error, got nil")
	}
}

func TestOpenRegistryRejectsDuplicateTopicsInFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "topics.json")
	data, _ := json.Marshal(map[string]any{
		"topics": []Topic{{Name: "a", Partitions: 1}, {Name: "a", Partitions: 2}},
	})
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := OpenRegistry(path); err == nil {
		t.Fatal("duplicate topics in file: want error, got nil")
	}
}

func TestOpenRegistryRejectsInvalidPartitionsInFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "topics.json")
	data, _ := json.Marshal(map[string]any{
		"topics": []Topic{{Name: "a", Partitions: 0}},
	})
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := OpenRegistry(path); err == nil {
		t.Fatal("invalid partition count in file: want error, got nil")
	}
}
