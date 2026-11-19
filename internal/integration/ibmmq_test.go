package integration

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// IBM MQ source: stub-tested with the same HTTP contract (peek then DELETE per record).
func TestIBMSourceStub(t *testing.T) {
	state := 0
	messages := []string{"m1", "m2", "m3"}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			if state >= len(messages) {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			w.Header().Set("ibm-mq-md-messageId", "ID:deadbeef")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(messages[state]))
		case http.MethodDelete:
			if state < len(messages) {
				state++
			}
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	t.Setenv("MQ_STUB_PASS", "secret-pw")
	src, err := NewIBMSinkSource("mq-src", ts.URL+"/ibmmq/rest/v3", "QM1", "EVENTS", "app", "MQ_STUB_PASS", "")
	if err != nil {
		t.Fatal(err)
	}
	// Empty position → first fetch browses head
	for _, want := range []string{"m1", "m2", "m3"} {
		recs, err := src.FetchBatch(context.Background(), "", 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(recs) != 1 || string(recs[0].Value) != want {
			t.Fatalf("fetch = %v, want %q", recs, want)
		}
		if err := src.Ack(context.Background(), recs); err != nil {
			t.Fatal(err)
		}
	}
	// After all consumed, DELETE on empty is tolerated (state beyond end) and fetch returns nothing.
	recs, err := src.FetchBatch(context.Background(), "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 0 {
		t.Fatalf("expected drained queue, got %v", recs)
	}
}
