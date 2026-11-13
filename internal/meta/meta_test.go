package meta

import "errors"
import "testing"

func TestCreateAndGet(t *testing.T) {
	r := NewRegistry()
	if err := r.Create("orders", 3); err != nil {
		t.Fatalf("create: %v", err)
	}
	top, err := r.Get("orders")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if top.Partitions != 3 {
		t.Fatalf("partitions: got %d, want 3", top.Partitions)
	}
}

func TestDuplicateTopicRejected(t *testing.T) {
	r := NewRegistry()
	_ = r.Create("orders", 1)
	if err := r.Create("orders", 1); !errors.Is(err, ErrTopicExists) {
		t.Fatalf("got %v, want ErrTopicExists", err)
	}
}

func TestInvalidPartitionsRejected(t *testing.T) {
	r := NewRegistry()
	if err := r.Create("bad", 0); !errors.Is(err, ErrInvalidPartitions) {
		t.Fatalf("got %v, want ErrInvalidPartitions", err)
	}
}

func TestMissingTopic(t *testing.T) {
	r := NewRegistry()
	if _, err := r.Get("nope"); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("got %v, want ErrTopicNotFound", err)
	}
}
