package ingest

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rahulkj/firn/internal/testutil"
	"github.com/rahulkj/firn/sdk/firngo"
)

func httpCfgFor(addr string) *HTTPConfig {
	cfg := &HTTPConfig{Listen: ":0"}
	cfg.Firn.Addr = addr
	cfg.Firn.Insecure = true
	cfg.BearerEnv = "TEST_INGEST_BEARER"
	cfg.RatePerMinute = 1000
	cfg.Routes = []HTTPRoute{{Name: "orders", Path: "/ingest/orders", Topic: "orders", KeyJSONPointer: "/order_id"}}
	return cfg
}

func TestHTTPIngestProducesWithKey(t *testing.T) {
	t.Setenv("TEST_INGEST_BEARER", "secret")
	baddr := testutil.StartBroker(t, map[string]int{"orders": 2, "ingest-dlq": 1})
	client, err := firngo.DialInsecure(context.Background(), baddr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	cfg := httpCfgFor(baddr)
	cfg.DLQTopic = "ingest-dlq"
	if _, err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	srv, err := NewServer(cfg, client)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// Unauthorized.
	resp, err := http.Post(ts.URL+"/ingest/orders", "application/json", strings.NewReader(`{"order_id":"o-1"}`))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no-auth status = %d", resp.StatusCode)
	}

	// Authorized; key comes from order_id.
	req, _ := http.NewRequest("POST", ts.URL+"/ingest/orders", strings.NewReader(`{"order_id":"o-42","total":99}`))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d body=%s", resp.StatusCode, body)
	}

	// Verify the record landed in Firn with the extracted key.
	found := false
	for p := int32(0); p < 2 && !found; p++ {
		recs, err := client.Fetch(context.Background(), "orders", p, 0, 10)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			if string(r.Key) == "o-42" && strings.Contains(string(r.Value), `"order_id":"o-42"`) {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("record with key o-42 not found in orders topic")
	}
}

func TestHTTPIngestTerminalErrorGoesToDLQ(t *testing.T) {
	t.Setenv("TEST_INGEST_BEARER", "secret")
	baddr := testutil.StartBroker(t, map[string]int{"orders": 1, "ingest-dlq": 1})
	client, _ := firngo.DialInsecure(context.Background(), baddr)
	defer func() { _ = client.Close() }()
	cfg := httpCfgFor(baddr)
	cfg.DLQTopic = "ingest-dlq"
	if _, err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	srv, _ := NewServer(cfg, client)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// Body is not JSON — key pointer cannot be resolved → DLQ.
	req, _ := http.NewRequest("POST", ts.URL+"/ingest/orders", strings.NewReader("not json"))
	req.Header.Set("Authorization", "Bearer secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	recs, err := client.Fetch(context.Background(), "ingest-dlq", 0, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || !strings.Contains(string(recs[0].Value), `"stage":"validate"`) {
		t.Fatalf("DLQ missing validate record: %+v", recs)
	}
}

func TestHTTPIngestHMAC(t *testing.T) {
	t.Setenv("TEST_INGEST_HMAC", "hmac-secret")
	baddr := testutil.StartBroker(t, map[string]int{"events": 1})
	client, _ := firngo.DialInsecure(context.Background(), baddr)
	defer func() { _ = client.Close() }()
	cfg := &HTTPConfig{Listen: ":0", HMACEnv: "TEST_INGEST_HMAC", RatePerMinute: 1000}
	cfg.Firn.Addr = baddr
	cfg.Firn.Insecure = true
	cfg.Routes = []HTTPRoute{{Name: "events", Path: "/hook", Topic: "events"}}
	if _, err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	srv, _ := NewServer(cfg, client)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	body := `{"hello":"world"}`
	mac := hmac.New(sha256.New, []byte("hmac-secret"))
	mac.Write([]byte(body))

	req, _ := http.NewRequest("POST", ts.URL+"/hook", strings.NewReader(body))
	req.Header.Set("X-Firn-Signature-256", hex.EncodeToString(mac.Sum(nil)))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("HMAC status = %d", resp.StatusCode)
	}
	// Wrong signature refused.
	req, _ = http.NewRequest("POST", ts.URL+"/hook", strings.NewReader(body))
	req.Header.Set("X-Firn-Signature-256", strings.Repeat("00", 32))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bad HMAC status = %d", resp.StatusCode)
	}
}

func TestHTTPConfigValidation(t *testing.T) {
	cfg := &HTTPConfig{}
	if _, err := cfg.Validate(); err == nil {
		t.Fatal("empty config should fail validation")
	}
	cfg = httpCfgFor("localhost:9461")
	cfg.BearerEnv = ""
	cfg.AllowAnonymous = false
	if _, err := cfg.Validate(); err == nil {
		t.Fatal("config without auth must fail closed")
	}
}

// ---- MQTT ----

func mqttPacket(pktType byte, flags byte, body []byte) []byte {
	out := []byte{pktType<<4 | flags}
	out = appendVarint(out, len(body))
	return append(out, body...)
}

func connectPacket(clientID, username, password string, keepAlive uint16) []byte {
	var b []byte
	appendStr := func(s string) {
		b = append(b, byte(len(s)>>8), byte(len(s)))
		b = append(b, s...)
	}
	appendStr("MQTT")
	b = append(b, 4) // level
	wireFlags := byte(0x02)
	if username != "" {
		wireFlags |= 0x80
	}
	if password != "" {
		wireFlags |= 0x40
	}
	b = append(b, wireFlags, byte(keepAlive>>8), byte(keepAlive))
	appendStr(clientID)
	if username != "" {
		appendStr(username)
	}
	if password != "" {
		b = append(b, byte(len(password)>>8), byte(len(password)))
		b = append(b, password...)
	}
	return mqttPacket(1, 0, b)
}

func publishPacket(qos byte, retain bool, topic string, packetID uint16, payload []byte) []byte {
	var b []byte
	b = append(b, byte(len(topic)>>8), byte(len(topic)))
	b = append(b, topic...)
	if qos > 0 {
		b = append(b, uint16Bytes(packetID)...)
	}
	b = append(b, payload...)
	flags := qos << 1
	if retain {
		flags |= 1
	}
	return mqttPacket(3, flags, b)
}

func readFullPacket(t *testing.T, conn net.Conn) (byte, []byte) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	head := make([]byte, 1)
	if _, err := io.ReadFull(conn, head); err != nil {
		t.Fatalf("read packet: %v", err)
	}
	remaining := 0
	mult := 1
	for {
		var b [1]byte
		if _, err := io.ReadFull(conn, b[:]); err != nil {
			t.Fatalf("read remaining len: %v", err)
		}
		remaining += int(b[0]&0x7f) * mult
		if b[0]&0x80 == 0 {
			break
		}
		mult *= 128
	}
	payload := make([]byte, remaining)
	if _, err := io.ReadFull(conn, payload); err != nil {
		t.Fatalf("read payload: %v", err)
	}
	return head[0] >> 4, payload
}

func mqttConfigFor(addr string) *MQTTConfig {
	cfg := &MQTTConfig{Listen: "127.0.0.1:0", AllowAnonymous: false, MaxConnections: 10}
	cfg.Firn.Addr = addr
	cfg.Firn.Insecure = true
	cfg.Users = []MQTTUser{{Username: "dev1", PasswordEnv: "TEST_MQTT_PW"}}
	cfg.Routes = []MQTTRoute{{Name: "telemetry", Pattern: "devices/+/telemetry", Topic: "iot", KeyFrom: "client_id"}}
	return cfg
}

func startMQTT(t *testing.T, baddr string) (net.Conn, *MQTTServer, string) {
	t.Helper()
	t.Setenv("TEST_MQTT_PW", "pw")
	cfg := mqttConfigFor(baddr)
	if _, err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	client, err := firngo.DialInsecure(context.Background(), baddr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	srv, err := NewMQTTServer(cfg, client)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	_ = probe.Close()
	cfg.Listen = addr
	go func() { _ = srv.ListenAndServe(ctx) }()
	// Wait for the listener to accept.
	var conn net.Conn
	for i := 0; ; i++ {
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			conn = c
			break
		}
		if i > 50 {
			t.Fatal("mqtt listener did not come up")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return conn, srv, addr
}

func TestMQTTBadCredentialsRefused(t *testing.T) {
	baddr := testutil.StartBroker(t, map[string]int{"iot": 1})
	conn, _, _ := startMQTT(t, baddr)
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write(connectPacket("client-1", "dev1", "wrong", 60)); err != nil {
		t.Fatal(err)
	}
	pktType, payload := readFullPacket(t, conn)
	if pktType != 2 || len(payload) != 2 || payload[1] != 0x04 {
		t.Fatalf("expected CONNACK 0x04, got type=%d payload=%v", pktType, payload)
	}
}

func TestMQTTPublishQoS1LandsInFirn(t *testing.T) {
	baddr := testutil.StartBroker(t, map[string]int{"iot": 1, "ingest-dlq": 1})
	conn, _, _ := startMQTT(t, baddr)
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write(connectPacket("dev-client-1", "dev1", "pw", 60)); err != nil {
		t.Fatal(err)
	}
	pktType, payload := readFullPacket(t, conn)
	if pktType != 2 || payload[1] != 0x00 {
		t.Fatalf("expected CONNACK accepted, got %d %v", pktType, payload)
	}
	if _, err := conn.Write(publishPacket(1, false, "devices/sensor/telemetry", 7, []byte(`{"temp":21.5}`))); err != nil {
		t.Fatal(err)
	}
	pktType, payload = readFullPacket(t, conn)
	if pktType != 4 || binary.BigEndian.Uint16(payload) != 7 {
		t.Fatalf("expected PUBACK packet id 7, got type=%d payload=%v", pktType, payload)
	}
	// Allow the produce to be visible.
	client, _ := firngo.DialInsecure(context.Background(), baddr)
	defer func() { _ = client.Close() }()
	var got []byte
	for i := 0; i < 50; i++ {
		recs, err := client.Fetch(context.Background(), "iot", 0, 0, 10)
		if err == nil && len(recs) == 1 {
			got = recs[0].Value
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !strings.Contains(string(got), `"temp":21.5`) {
		t.Fatalf("record not in firn: %q", got)
	}
}

func TestMQTTUnmappedTopicClosesConnection(t *testing.T) {
	baddr := testutil.StartBroker(t, map[string]int{"iot": 1})
	conn, _, _ := startMQTT(t, baddr)
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write(connectPacket("c1", "dev1", "pw", 60)); err != nil {
		t.Fatal(err)
	}
	readFullPacket(t, conn) // CONNACK
	if _, err := conn.Write(publishPacket(1, false, "elsewhere/nowhere", 9, []byte("x"))); err != nil {
		t.Fatal(err)
	}
	// No PUBACK; server closes. Read should hit EOF/reset.
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err := conn.Read(make([]byte, 1))
	if err == nil || err != io.EOF && !strings.Contains(err.Error(), "connection") {
		// A reset is acceptable too; must not be a PUBACK.
		t.Fatalf("expected connection close, got %v", err)
	}
}

func TestMQTTSubscribeRefused(t *testing.T) {
	baddr := testutil.StartBroker(t, map[string]int{"iot": 1})
	conn, _, _ := startMQTT(t, baddr)
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write(connectPacket("c2", "dev1", "pw", 60)); err != nil {
		t.Fatal(err)
	}
	readFullPacket(t, conn) // CONNACK
	// SUBSCRIBE: packet id 0x0001, one filter "iot" + requested QoS 0.
	var body []byte
	body = append(body, 0x00, 0x01, 0x00, 0x03, 'i', 'o', 't', 0x00)
	if _, err := conn.Write(mqttPacket(8, 0x02, body)); err != nil {
		t.Fatal(err)
	}
	pktType, payload := readFullPacket(t, conn)
	if pktType != 9 || len(payload) != 3 || payload[2] != 0x80 {
		t.Fatalf("expected SUBACK 0x80, got type=%d payload=%v", pktType, payload)
	}
}

func TestMQTTFilterMatching(t *testing.T) {
	for _, tc := range []struct {
		filter, topic string
		want          bool
	}{
		{"devices/+/telemetry", "devices/a/telemetry", true},
		{"devices/+/telemetry", "devices/a/b/telemetry", false},
		{"devices/#", "devices/a/b", true},
		{"a/+/c", "a/b/c", true},
		{"a/+/c", "a/b/d", false},
		{"#", "anything", true},
		{"exact", "exact", true},
		{"exact", "exact/nope", false},
	} {
		if got := matchMQTTFilter(tc.filter, tc.topic); got != tc.want {
			t.Errorf("matchMQTTFilter(%q, %q) = %v, want %v", tc.filter, tc.topic, got, tc.want)
		}
	}
	if err := validateMQTTFilter("devices/#/extra"); err == nil {
		t.Error("trailing # rule not enforced")
	}
}

func TestMQTTConfigFailsClosed(t *testing.T) {
	cfg := &MQTTConfig{}
	if _, err := cfg.Validate(); err == nil {
		t.Fatal("missing firn addr must fail")
	}
	cfg = mqttConfigFor("localhost:9461")
	cfg.Users = nil
	cfg.AllowAnonymous = false
	if _, err := cfg.Validate(); err == nil {
		t.Fatal("no users and no anonymous must fail closed")
	}
}
