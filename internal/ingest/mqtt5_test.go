package ingest

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/rahulkj/firn/internal/testutil"
	"github.com/rahulkj/firn/sdk/firngo"
)

func connectPacketV5(clientID, username, password, authMethod string) []byte {
	var b []byte
	b = append(b, 0x00, 0x04, 'M', 'Q', 'T', 'T') // protocol name
	b = append(b, 5)                              // level
	wireFlags := byte(0x02)
	if username != "" {
		wireFlags |= 0x80
	}
	if password != "" {
		wireFlags |= 0x40
	}
	b = append(b, wireFlags, 0x00, 0x3c) // flags, keepalive
	props := []byte{}
	if authMethod != "" {
		props = append(props, 0x15)
		props = append(props, byte(len(authMethod)>>8), byte(len(authMethod)))
		props = append(props, authMethod...)
	}
	b = append(b, appendVarint(nil, len(props))...)
	b = append(b, props...)
	addStr := func(s string) {
		b = append(b, byte(len(s)>>8), byte(len(s)))
		b = append(b, s...)
	}
	addStr(clientID)
	if username != "" {
		addStr(username)
	}
	if password != "" {
		b = append(b, byte(len(password)>>8), byte(len(password)))
		b = append(b, password...)
	}
	return mqttPacket(1, 0, b)
}

func publishPacketV5(qos byte, topic string, packetID uint16, payload []byte, props []byte) []byte {
	var b []byte
	b = append(b, byte(len(topic)>>8), byte(len(topic)))
	b = append(b, topic...)
	if qos > 0 {
		b = append(b, uint16Bytes(packetID)...)
	}
	b = append(b, appendVarint(nil, len(props))...)
	b = append(b, props...)
	b = append(b, payload...)
	flags := qos << 1
	return mqttPacket(3, flags, b)
}

func subscribePacketV5(packetID uint16, filters []string) []byte {
	var b []byte
	b = append(b, uint16Bytes(packetID)...)
	b = append(b, 0x00) // properties length = 0
	for _, f := range filters {
		b = append(b, byte(len(f)>>8), byte(len(f)))
		b = append(b, f...)
		b = append(b, 0x00) // options: QoS 0, no flags
	}
	return mqttPacket(8, 0x02, b)
}

func v5PublishContentTypeProp() []byte {
	// content-type "application/json" (0x03, string) + user property pair
	s := "application/json"
	p := []byte{0x03, byte(len(s) >> 8), byte(len(s))}
	p = append(p, s...)
	k, v := "origin", "test"
	p = append(p, 0x26, byte(len(k)>>8), byte(len(k)))
	p = append(p, k...)
	p = append(p, byte(len(v)>>8), byte(len(v)))
	p = append(p, v...)
	return p
}

func TestMQTT5ConnectAccepted(t *testing.T) {
	baddr := startMQTTBroker(t, map[string]string{"dev1": "pw"}, "devices/+/telemetry", "iot")
	conn, err := net.DialTimeout("tcp", baddr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write(connectPacketV5("c1", "dev1", "pw", "")); err != nil {
		t.Fatal(err)
	}
	pktType, payload := readFullPacket(t, conn)
	if pktType != 2 {
		t.Fatalf("expected CONNACK (2), got %d", pktType)
	}
	if payload[0] != 0x00 || payload[1] != 0x00 {
		t.Fatalf("CONNACK flags/reason = %x/%x", payload[0], payload[1])
	}
	// CONNACK must carry properties (session expiry at least) in v5.
	props, rest, err := parseProperties(payload[2:])
	if err != nil {
		t.Fatalf("CONNACK properties invalid: %v", err)
	}
	if len(rest) != 0 {
		t.Fatalf("CONNACK trailing bytes: %v", rest)
	}
	foundSessionExpiry := false
	for _, p := range props {
		if p.id == 0x11 {
			foundSessionExpiry = true
		}
	}
	if !foundSessionExpiry {
		t.Fatalf("CONNACK missing session expiry property: %v", props)
	}
}

func TestMQTT5BadCredentialsReasonCode(t *testing.T) {
	baddr := startMQTTBroker(t, map[string]string{"dev1": "pw"}, "devices/+/telemetry", "iot")
	conn, _ := net.DialTimeout("tcp", baddr, 2*time.Second)
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write(connectPacketV5("c1", "dev1", "wrong", "")); err != nil {
		t.Fatal(err)
	}
	_, payload := readFullPacket(t, conn)
	if payload[1] != 0x84 {
		t.Fatalf("reason = 0x%02x, want 0x84", payload[1])
	}
}

func TestMQTT5ExtendedAuthRefused(t *testing.T) {
	baddr := startMQTTBroker(t, map[string]string{"dev1": "pw"}, "devices/+/telemetry", "iot")
	conn, _ := net.DialTimeout("tcp", baddr, 2*time.Second)
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write(connectPacketV5("c1", "dev1", "pw", "SCRAM-SHA-256")); err != nil {
		t.Fatal(err)
	}
	_, payload := readFullPacket(t, conn)
	if payload[1] != 0x86 {
		t.Fatalf("reason = 0x%02x, want 0x86 (bad authentication method)", payload[1])
	}
}

func TestMQTT5PublishWithProperties(t *testing.T) {
	baddr := startMQTTBroker(t, map[string]string{"dev1": "pw"}, "devices/+/telemetry", "iot")
	conn, _ := net.DialTimeout("tcp", baddr, 2*time.Second)
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write(connectPacketV5("dev-client-1", "dev1", "pw", "")); err != nil {
		t.Fatal(err)
	}
	readFullPacket(t, conn) // CONNACK
	pkt := publishPacketV5(1, "devices/room/telemetry", 42, []byte(`{"temp":20}`), v5PublishContentTypeProp())
	if _, err := conn.Write(pkt); err != nil {
		t.Fatal(err)
	}
	pktType, payload := readFullPacket(t, conn)
	if pktType != 4 {
		t.Fatalf("expected PUBACK, got %d", pktType)
	}
	if binary.BigEndian.Uint16(payload[:2]) != 42 || payload[2] != 0x00 {
		t.Fatalf("PUBACK pid/reason = %x", payload)
	}
}

func TestMQTT5SubscribeRefusedWithReasonCodes(t *testing.T) {
	baddr := startMQTTBroker(t, map[string]string{"dev1": "pw"}, "devices/+/telemetry", "iot")
	conn, _ := net.DialTimeout("tcp", baddr, 2*time.Second)
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write(connectPacketV5("c2", "dev1", "pw", "")); err != nil {
		t.Fatal(err)
	}
	readFullPacket(t, conn) // CONNACK
	if _, err := conn.Write(subscribePacketV5(7, []string{"devices/+/telemetry", "other/#"})); err != nil {
		t.Fatal(err)
	}
	pktType, payload := readFullPacket(t, conn)
	if pktType != 9 {
		t.Fatalf("expected SUBACK, got %d", pktType)
	}
	// SUBACK 5.0: [pktID(2), propertiesLen(1), reason1, reason2]
	if len(payload) < 5 || payload[0] != 0x00 || payload[1] != 0x07 {
		t.Fatalf("SUBACK malformed: %v", payload)
	}
	propsLen := int(payload[2])
	grants := payload[3+propsLen:]
	if len(grants) != 2 || grants[0] != 0x87 || grants[1] != 0x87 {
		t.Fatalf("SUBACK grants = %v, want [0x87 0x87]", grants)
	}
}

func TestMQTT5QoS2Flow(t *testing.T) {
	baddr := startMQTTBroker(t, map[string]string{"dev1": "pw"}, "devices/+/telemetry", "iot")
	conn, _ := net.DialTimeout("tcp", baddr, 2*time.Second)
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write(connectPacketV5("c3", "dev1", "pw", "")); err != nil {
		t.Fatal(err)
	}
	readFullPacket(t, conn) // CONNACK
	if _, err := conn.Write(publishPacketV5(2, "devices/x/telemetry", 100, []byte("q2-body"), nil)); err != nil {
		t.Fatal(err)
	}
	pktType, _ := readFullPacket(t, conn)
	if pktType != 5 { // PUBREC
		t.Fatalf("expected PUBREC, got %d", pktType)
	}
	// PUBREL 5.0: [packetID(2), reason(1), propertiesLen(1)=0]
	if _, err := conn.Write(mqttPacket(6, 0x02, []byte{0x00, 100, 0x00, 0x00})); err != nil {
		t.Fatal(err)
	}
	pktType, payload := readFullPacket(t, conn)
	if pktType != 7 { // PUBCOMP
		t.Fatalf("expected PUBCOMP, got %d", pktType)
	}
	if binary.BigEndian.Uint16(payload[:2]) != 100 || payload[2] != 0x00 {
		t.Fatalf("PUBCOMP pid/reason = %v", payload)
	}
}

// Helper: spins up an in-process MQTT gateway against a fresh in-process broker.
func startMQTTBroker(t *testing.T, users map[string]string, pattern, targetTopic string) string {
	t.Helper()
	baddr := testutil.StartBroker(t, map[string]int{targetTopic: 1})
	cfg := &MQTTConfig{Listen: "127.0.0.1:0", AllowAnonymous: false}
	cfg.Firn.Addr = baddr
	cfg.Firn.Insecure = true
	for u := range users {
		cfg.Users = append(cfg.Users, MQTTUser{Username: u, PasswordEnv: "MQTT5_TEST_PW"})
	}
	t.Setenv("MQTT5_TEST_PW", firstValue(users))
	cfg.Routes = []MQTTRoute{{Name: "r1", Pattern: pattern, Topic: targetTopic, KeyFrom: "client_id"}}
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
	// Bind an ephemeral port, learn it, release it, hand to the server.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	_ = probe.Close()
	cfg.Listen = addr
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = srv.ListenAndServe(ctx) }()
	for i := 0; ; i++ {
		c, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			_ = c.Close()
			break
		}
		if i > 60 {
			t.Fatal("mqtt listener did not come up")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return addr
}

func firstValue(m map[string]string) string {
	for _, v := range m {
		return v
	}
	return ""
}
