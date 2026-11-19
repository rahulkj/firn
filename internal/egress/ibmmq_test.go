package egress

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// IBM MQ sink: stub-tested, not live-verified (the mqweb REST API shape is
// verified against IBM docs; the ibmcom/mq image does not run on arm64
// hosts). Asserts: Basic auth, CSRF header, Content-Type, persistent flag,
// body passthrough, error mapping.
func TestIBMSinkStub(t *testing.T) {
	var gotAuth, gotCSRF, gotCT, gotPersistence, gotCorr string
	var body []byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			t.Fatalf("method = %s", r.Method)
		}
		gotAuth = r.Header.Get("Authorization")
		gotCSRF = r.Header.Get("ibm-mq-rest-csrf-token")
		gotCT = r.Header.Get("Content-Type")
		gotPersistence = r.Header.Get("ibm-mq-md-persistence")
		gotCorr = r.Header.Get("ibm-mq-md-correlationId")
		body, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer ts.Close()

	t.Setenv("MQ_STUB_PASS", "secret-pw")
	r := Route{Name: "mq-out"}
	r.Sink = SinkConfig{
		Type: "ibmmq", MQWebURL: ts.URL + "/ibmmq/rest/v3", MQWebQueueMgr: "QM1", MQWebQueue: "EVENTS",
		MQWebUser: "app", MQWebPassEnv: "MQ_STUB_PASS",
	}
	sink, err := NewSink(r)
	if err != nil {
		t.Fatal(err)
	}
	batch := []Delivery{{Value: []byte("hello-mq"), Offset: 0, Partition: 0, Key: []byte("k1")}}
	if err := sink.Write(context.Background(), "mq-out", batch); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(gotAuth, "Basic ") {
		t.Fatalf("Authorization = %q", gotAuth)
	}
	if gotCSRF == "" {
		t.Fatal("csrf token header missing")
	}
	if !strings.HasPrefix(gotCT, "text/plain") {
		t.Fatalf("Content-Type = %q", gotCT)
	}
	if gotPersistence != "persistent" {
		t.Fatalf("persistence = %q", gotPersistence)
	}
	if !strings.HasPrefix(gotCorr, "ID:") {
		t.Fatalf("correlation id = %q", gotCorr)
	}
	if string(body) != "hello-mq" {
		t.Fatalf("body = %q", body)
	}
	// 4xx → terminal (do not retry), 5xx → retryable
	for code, wantTerminal := range map[int]bool{http.StatusForbidden: true, http.StatusBadRequest: true, http.StatusServiceUnavailable: false} {
		ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(code)
		}))
		r2 := Route{Name: "mq2", Sink: SinkConfig{
			Type: "ibmmq", MQWebURL: ts2.URL + "/ibmmq/rest/v3", MQWebQueueMgr: "QM1", MQWebQueue: "E",
			MQWebUser: "u", MQWebPassEnv: "MQ_STUB_PASS",
		}}
		s2, err := NewSink(r2)
		if err != nil {
			t.Fatal(err)
		}
		batch := []Delivery{{Value: []byte("x"), Offset: 0, Partition: 0}}
		err = s2.Write(context.Background(), "mq2", batch)
		var te *TerminalError
		gotTerminal := err != nil && errorsAsAs(err, &te)
		if gotTerminal != wantTerminal {
			t.Fatalf("code %d: terminal=%v want=%v (err=%v)", code, gotTerminal, wantTerminal, err)
		}
		ts2.Close()
	}
}

