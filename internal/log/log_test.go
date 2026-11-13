package log

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestAppendAssignsSequentialOffsets(t *testing.T) {
	var l MemLog
	for i := int64(0); i < 100; i++ {
		if got := l.Append(nil, []byte(fmt.Sprintf("v-%d", i))); got != i {
			t.Fatalf("append %d: got offset %d", i, got)
		}
	}
	if hw := l.HighWatermark(); hw != 100 {
		t.Fatalf("high watermark: got %d, want 100", hw)
	}
}

func TestFetchReturnsOrderedRecords(t *testing.T) {
	var l MemLog
	for i := 0; i < 10; i++ {
		l.Append([]byte("k"), []byte(fmt.Sprintf("v-%d", i)))
	}
	recs, err := l.Fetch(3, 4)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(recs) != 4 || recs[0].Offset != 3 || recs[3].Offset != 6 {
		t.Fatalf("unexpected records: %+v", recs)
	}
}

func TestFetchRejectsBadOffsets(t *testing.T) {
	var l MemLog
	l.Append(nil, []byte("x"))
	if _, err := l.Fetch(-1, 1); err == nil {
		t.Fatal("negative offset: want error")
	}
	if _, err := l.Fetch(1, 1); err == nil {
		t.Fatal("offset beyond end: want error")
	}
}

// TestLogRoundTripProperty is the seed of the property-test suite required
// by AGENTS.md. Replace with a real property-test library (e.g. gopter)
// once the file-backed log lands — semantics must hold for crashes too.
func TestLogRoundTripProperty(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for trial := 0; trial < 50; trial++ {
		var l MemLog
		n := 1 + rng.Intn(200)
		for i := 0; i < n; i++ {
			l.Append(nil, []byte{byte(i)})
		}
		recs, err := l.Fetch(0, n)
		if err != nil {
			t.Fatalf("trial %d: fetch: %v", trial, err)
		}
		for i, r := range recs {
			if r.Offset != int64(i) || len(r.Value) != 1 || int(r.Value[0]) != i {
				t.Fatalf("trial %d: record %d corrupt: %+v", trial, i, r)
			}
		}
	}
}
