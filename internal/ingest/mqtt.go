package ingest

import (
	"context"
	"crypto/subtle"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"github.com/rahulkj/firn/internal/integration"
	"github.com/rahulkj/firn/sdk/firngo"
)

// MQTTConfig is the versioned configuration for the MQTT ingress gateway.
// The gateway is publish-only by design (ADR-016): devices publish into
// Firn; subscriptions are refused (SUBACK 0x80). This is an MQTT 3.1.1
// endpoint — MQTT 5.0 clients connect in 3.1.1 mode. Delivery is
// at-least-once end-to-end; MQTT QoS 2 does not make it exactly-once.
type MQTTConfig struct {
	Firn struct {
		Addr      string `json:"addr"`
		APIKeyEnv string `json:"api_key_env"`
		CAEnv     string `json:"ca_env"`
		Insecure  bool   `json:"insecure"`
	} `json:"firn"`
	Listen         string      `json:"listen"` // default :1883
	AllowAnonymous bool        `json:"allow_anonymous"`
	MaxConnections int         `json:"max_connections"`  // default 1000
	MaxPacketBytes int         `json:"max_packet_bytes"` // default 256 KiB
	Users          []MQTTUser  `json:"users"`            // empty + !AllowAnonymous ⇒ all refused
	Routes         []MQTTRoute `json:"routes"`
	DLQTopic       string      `json:"dlq_topic"`
}

// MQTTUser is one device credential. Passwords come from env vars, never
// inline in the config file.
type MQTTUser struct {
	Username    string `json:"username"`
	PasswordEnv string `json:"password_env"`
}

// MQTTRoute maps an MQTT topic filter to a Firn topic. Patterns support
// '+' (single level) and trailing '#' (multi level). KeyFrom selects the
// record key: client_id (default), username, or none (round-robin).
type MQTTRoute struct {
	Name    string `json:"name"`
	Pattern string `json:"pattern"`
	Topic   string `json:"topic"`
	KeyFrom string `json:"key_from"`
}

// LoadMQTTConfig reads and validates the JSON config at path.
func LoadMQTTConfig(path string) (*MQTTConfig, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("mqtt: read config: %w", err)
	}
	var cfg MQTTConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, fmt.Errorf("mqtt: parse config %s: %w", path, err)
	}
	return cfg.Validate()
}

// Validate checks the config and normalizes defaults.
func (c *MQTTConfig) Validate() (*MQTTConfig, error) {
	if c.Listen == "" {
		c.Listen = ":1883"
	}
	if c.Firn.Addr == "" {
		return nil, errors.New("mqtt: firn.addr required")
	}
	if c.MaxConnections <= 0 {
		c.MaxConnections = 1000
	}
	if c.MaxPacketBytes <= 0 {
		c.MaxPacketBytes = 256 * 1024
	}
	if !c.AllowAnonymous && len(c.Users) == 0 {
		return nil, errors.New("mqtt: refusing to start: allow_anonymous false and no users configured")
	}
	for i, u := range c.Users {
		if u.Username == "" || u.PasswordEnv == "" {
			return nil, fmt.Errorf("mqtt: users[%d]: username and password_env required", i)
		}
		if os.Getenv(u.PasswordEnv) == "" {
			return nil, fmt.Errorf("mqtt: users[%d]: env %s is empty or unset", i, u.PasswordEnv)
		}
	}
	seen := map[string]bool{}
	for i, r := range c.Routes {
		if !integration.ValidName(r.Name) {
			return nil, fmt.Errorf("mqtt: routes[%d]: invalid name %q", i, r.Name)
		}
		if r.Pattern == "" || r.Topic == "" {
			return nil, fmt.Errorf("mqtt: routes[%d]: pattern and topic required", i)
		}
		if err := validateMQTTFilter(r.Pattern); err != nil {
			return nil, fmt.Errorf("mqtt: routes[%d]: %w", i, err)
		}
		if seen[r.Pattern] {
			return nil, fmt.Errorf("mqtt: duplicate pattern %q", r.Pattern)
		}
		seen[r.Pattern] = true
		switch r.KeyFrom {
		case "", "client_id", "username", "none":
		default:
			return nil, fmt.Errorf("mqtt: routes[%d]: key_from must be client_id|username|none", i)
		}
	}
	if len(c.Routes) == 0 {
		return nil, errors.New("mqtt: at least one route required")
	}
	return c, nil
}

func validateMQTTFilter(s string) error {
	if strings.Contains(s, "#") && !strings.HasSuffix(s, "/#") && s != "#" {
		return fmt.Errorf("pattern %q: '#' must be the final level", s)
	}
	for _, lvl := range strings.Split(s, "/") {
		if strings.Contains(lvl, "#") && lvl != "#" {
			return fmt.Errorf("pattern %q: '#' must occupy the whole level", s)
		}
		if strings.Contains(lvl, "+") && lvl != "+" {
			return fmt.Errorf("pattern %q: '+' must occupy the whole level", s)
		}
	}
	return nil
}

// matchMQTTFilter reports whether topic matches an MQTT filter.
func matchMQTTFilter(filter, topic string) bool {
	fl := strings.Split(filter, "/")
	tl := strings.Split(topic, "/")
	for i, l := range fl {
		if l == "#" {
			return true
		}
		if i >= len(tl) {
			return false
		}
		if l != "+" && l != tl[i] {
			return false
		}
	}
	return len(fl) == len(tl)
}

// MQTTServer is the publish-ingest listener.
type MQTTServer struct {
	cfg     *MQTTConfig
	client  *firngo.Client
	connSem chan struct{}
	users   map[string]string // username -> resolved password
	Metrics *integration.Metrics
}

// NewMQTTServer builds the server (does not listen).
func NewMQTTServer(cfg *MQTTConfig, client *firngo.Client) (*MQTTServer, error) {
	users := map[string]string{}
	for _, u := range cfg.Users {
		users[u.Username] = os.Getenv(u.PasswordEnv)
	}
	s := &MQTTServer{cfg: cfg, client: client, connSem: make(chan struct{}, cfg.MaxConnections), users: users}
	s.Metrics = integration.NewMetrics("ingest")
	return s, nil
}

// ListenAndServe serves MQTT until ctx is cancelled.
func (s *MQTTServer) ListenAndServe(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		return fmt.Errorf("mqtt: listen %s: %w", s.cfg.Listen, err)
	}
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	for {
		c, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("mqtt: accept: %w", err)
		}
		select {
		case s.connSem <- struct{}{}:
		default:
			_ = c.Close() // connection cap reached; drop
			continue
		}
		s.Metrics.ConnectionsActive.WithLabelValues("ingest", "mqtt").Inc()
		go func() {
			defer func() { <-s.connSem }()
			defer s.Metrics.ConnectionsActive.WithLabelValues("ingest", "mqtt").Dec()
			s.serveConn(ctx, c)
		}()
	}
}

type connectInfo struct {
	clientID   string
	username   string
	password   string
	version    int    // 4 = MQTT 3.1.1, 5 = MQTT 5.0
	authMethod string // v5 extended-auth method (must be empty — unsupported)
}

func (s *MQTTServer) serveConn(ctx context.Context, conn net.Conn) {
	defer func() { _ = conn.Close() }()
	_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))
	pktType, _, payload, err := s.readPacket(conn)
	if err != nil || pktType != 1 { // CONNECT
		return
	}
	ci, err := parseConnectAuto(payload)
	if err != nil {
		reason := byte(0x82)
		var ce *connectError
		if errors.As(err, &ce) {
			reason = ce.reason
		}
		s.writeConnAck(conn, ci, reason, err.Error())
		return
	}
	if !s.authenticate(ci) {
		s.writeConnAck(conn, ci, 0x84, "bad username or password")
		return
	}
	s.writeConnAck(conn, ci, 0x00, "")

	for {
		_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		pktType, flags, payload, err := s.readPacket(conn)
		if err != nil {
			return
		}
		switch pktType {
		case 3: // PUBLISH
			s.handlePublish(ctx, conn, ci, flags, payload)
		case 6: // PUBREL — QoS 2 second leg; respond PUBCOMP
			if ci.version == 5 {
				if len(payload) < 2 {
					return
				}
				pid := append([]byte{}, payload[:2]...)
				s.writePacket(conn, 0x70, append(pid, 0x00, 0x00)) // PUBCOMP: reason 0x00, no props
				return
			}
			if len(payload) != 2 {
				return
			}
			s.writePacket(conn, 0x70, payload) // PUBCOMP with same packet id
		case 8: // SUBSCRIBE — refused: this is a publish-only ingress gateway
			if !s.handleSubscribeRefusal(conn, ci, payload) {
				return
			}
		case 12: // PINGREQ
			s.writePacket(conn, 0xD0, nil) // PINGRESP
		case 14: // DISCONNECT
			return
		default:
			return // fail-closed on anything unexpected
		}
	}
}

func (s *MQTTServer) writeConnAck(conn net.Conn, ci connectInfo, reasonCode byte, reason string) {
	if ci.version == 5 {
		body := append([]byte{0x00, reasonCode}, connAckProps(reasonCode, reason)...)
		s.writePacket(conn, 0x20, body)
		return
	}
	// v4 CONNACK return codes: 0x00 accepted, 0x01 bad protocol version,
	// 0x02 identifier rejected, 0x04 bad username/password, 0x05 not authorized.
	v4 := map[byte]byte{0x82: 0x02, 0x84: 0x04, 0x87: 0x05, 0x86: 0x05}
	if v4code, ok := v4[reasonCode]; ok {
		reasonCode = v4code
	}
	s.writePacket(conn, 0x20, []byte{0x00, reasonCode})
}

func (s *MQTTServer) authenticate(ci connectInfo) bool {
	if s.cfg.AllowAnonymous {
		return true
	}
	pw, ok := s.users[ci.username]
	if !ok || ci.username == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(pw), []byte(ci.password)) == 1
}

func (s *MQTTServer) handlePublish(ctx context.Context, conn net.Conn, ci connectInfo, flags byte, payload []byte) {
	qos := (flags >> 1) & 0x03
	topic, n, err := readPrefixedString(payload)
	if err != nil || topic == "" {
		_ = conn.Close()
		return
	}
	rest := payload[n:]
	var packetID uint16
	if qos > 0 {
		if len(rest) < 2 {
			_ = conn.Close()
			return
		}
		packetID = binary.BigEndian.Uint16(rest)
		rest = rest[2:]
	}
	if ci.version == 5 {
		// PUBLISH carries a property section before the payload in v5.
		_, remaining, err := parseProperties(rest)
		if err != nil {
			_ = conn.Close() // fail-closed on malformed properties
			return
		}
		rest = remaining
	}
	route, ok := s.routeFor(topic)
	if !ok {
		// Unmapped topic: no route to forward to. Closing the connection
		// (no PUBACK) keeps the client's message for retry instead of
		// silently dropping it; operators see the missing-route metric/log.
		_ = conn.Close()
		return
	}
	key := recordKeyFor(route, ci)
	produceStart := time.Now()
	s.Metrics.RecordsIn.WithLabelValues("ingest", route.Name, "mqtt", "ok").Add(1)
	_, _, err = s.produceWithRetry(ctx, route.Topic, key, rest)
	s.Metrics.OpSeconds.WithLabelValues("ingest", "produce").Observe(time.Since(produceStart).Seconds())
	if err == nil {
		s.Metrics.RecordsProduced.WithLabelValues("ingest", route.Name, route.Topic, "ok").Add(1)
	}
	if err != nil {
		s.Metrics.DLQTotal.WithLabelValues("ingest", route.Name, "produce").Add(1)
		if dlqErr := s.deadLetter(ctx, route, topic, key, rest, err); dlqErr != nil {
			// Produce failed and DLQ unavailable (or absent): close
			// without ack; at-least-once clients will retry.
			_ = conn.Close()
			return
		}
	}
	switch qos {
	case 1:
		if ci.version == 5 {
			s.writePacket(conn, 0x40, append(uint16Bytes(packetID), 0x00, 0x00)) // PUBACK: reason 0x00, no props
			return
		}
		s.writePacket(conn, 0x40, uint16Bytes(packetID)) // PUBACK
	case 2:
		if ci.version == 5 {
			s.writePacket(conn, 0x50, append(uint16Bytes(packetID), 0x00, 0x00)) // PUBREC: reason 0x00, no props
			return
		}
		s.writePacket(conn, 0x50, uint16Bytes(packetID)) // PUBREC (PUBREL -> PUBCOMP)
	}
}

func (s *MQTTServer) handleSubscribeRefusal(conn net.Conn, ci connectInfo, payload []byte) bool {
	if len(payload) < 2 {
		return false
	}
	pktID := payload[:2]
	rest := payload[2:]
	if ci.version == 5 {
		_, remaining, err := parseProperties(rest)
		if err != nil {
			return false
		}
		rest = remaining
	}
	var grants []byte
	for len(rest) > 0 {
		_, n, err := readPrefixedString(rest) // filter
		if err != nil {
			return false
		}
		rest = rest[n:]
		if len(rest) < 1 { // options byte (both versions have one here)
			return false
		}
		rest = rest[1:]
		if ci.version == 5 {
			grants = append(grants, 0x87) // not authorized (publish-only gateway)
			continue
		}
		grants = append(grants, 0x80) // failure (v4)
	}
	if ci.version == 5 {
		body := append(append([]byte{}, pktID...), 0x00) // properties length 0
		body = append(body, grants...)
		s.writePacket(conn, 0x90, body)
		return true
	}
	body := append(append([]byte{}, pktID...), grants...)
	s.writePacket(conn, 0x90, body)
	return true
}

func (s *MQTTServer) produceWithRetry(ctx context.Context, topic string, key, value []byte) (int32, int64, error) {
	p, o, err := s.client.Produce(ctx, topic, key, value)
	if err == nil {
		return p, o, nil
	}
	return s.client.Produce(ctx, topic, key, value)
}

func (s *MQTTServer) deadLetter(ctx context.Context, route MQTTRoute, srcTopic string, key, value []byte, cause error) error {
	if s.cfg.DLQTopic == "" {
		return cause
	}
	payload := []byte(fmt.Sprintf(`{"connector":"mqtt-ingest:%s","stage":"produce","source_topic":%q,"error":%q}`, route.Name, srcTopic, cause.Error()))
	_, _, err := s.client.Produce(ctx, s.cfg.DLQTopic, key, payload)
	return err
}

func (s *MQTTServer) routeFor(topic string) (MQTTRoute, bool) {
	for _, r := range s.cfg.Routes {
		if matchMQTTFilter(r.Pattern, topic) {
			return r, true
		}
	}
	return MQTTRoute{}, false
}

func recordKeyFor(r MQTTRoute, ci connectInfo) []byte {
	switch r.KeyFrom {
	case "username":
		if ci.username == "" {
			return nil
		}
		return []byte(ci.username)
	case "none":
		return nil
	default: // client_id
		if ci.clientID == "" {
			return nil
		}
		return []byte(ci.clientID)
	}
}

func uint16Bytes(v uint16) []byte { return []byte{byte(v >> 8), byte(v)} }

func (s *MQTTServer) readPacket(conn net.Conn) (pktType byte, flags byte, payload []byte, err error) {
	var head [1]byte
	if _, err = io.ReadFull(conn, head[:]); err != nil {
		return 0, 0, nil, err
	}
	pktType, flags = head[0]>>4, head[0]&0x0f
	remaining := 0
	mult := 1
	for i := 0; ; i++ {
		if i >= 4 {
			return 0, 0, nil, errors.New("mqtt: malformed remaining length")
		}
		var b [1]byte
		if _, err = io.ReadFull(conn, b[:]); err != nil {
			return 0, 0, nil, err
		}
		remaining += int(b[0]&0x7f) * mult
		if b[0]&0x80 == 0 {
			break
		}
		mult *= 128
	}
	if remaining > s.cfg.MaxPacketBytes {
		return 0, 0, nil, fmt.Errorf("mqtt: packet exceeds %d bytes", s.cfg.MaxPacketBytes)
	}
	payload = make([]byte, remaining)
	_, err = io.ReadFull(conn, payload)
	return pktType, flags, payload, err
}

func (s *MQTTServer) writePacket(conn net.Conn, header byte, body []byte) {
	pkt := []byte{header}
	pkt = appendVarint(pkt, len(body))
	pkt = append(pkt, body...)
	_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, _ = conn.Write(pkt)
}

func appendVarint(b []byte, v int) []byte {
	for {
		d := byte(v % 128)
		v /= 128
		if v > 0 {
			d |= 0x80
		}
		b = append(b, d)
		if v == 0 {
			return b
		}
	}
}

// parseConnect decodes an MQTT CONNECT payload (protocol name, version,
// flags, keepalive, then client id / will / username / password).
func parseConnect(payload []byte) (connectInfo, error) {
	var ci connectInfo
	ci.version = 4
	r := payload
	_, n, err := readPrefixedString(r)
	if err != nil {
		return ci, fmt.Errorf("connect: protocol name: %w", err)
	}
	r = r[n:]
	if len(r) < 4 {
		return ci, errors.New("connect: truncated header")
	}
	level := r[0]
	if level != 4 {
		return ci, fmt.Errorf("connect: unsupported protocol level %d (3.1.1 only)", level)
	}
	connectFlags := r[1]
	r = r[4:] // skip level(1) + flags(1) + keepalive(2)
	hasUsername := connectFlags&0x80 != 0
	hasPassword := connectFlags&0x40 != 0
	hasWill := connectFlags&0x04 != 0
	clientID, n, err := readPrefixedString(r)
	if err != nil || clientID == "" {
		return ci, fmt.Errorf("connect: client id: %w", err)
	}
	ci.clientID = clientID
	r = r[n:]
	if hasWill {
		_, n, err = readPrefixedString(r) // will topic
		if err != nil {
			return ci, fmt.Errorf("connect: will topic: %w", err)
		}
		r = r[n:]
		_, n, err = readBinary(r) // will message
		if err != nil {
			return ci, fmt.Errorf("connect: will message: %w", err)
		}
		r = r[n:]
	}
	if hasUsername {
		u, n, err := readPrefixedString(r)
		if err != nil {
			return ci, fmt.Errorf("connect: username: %w", err)
		}
		ci.username = u
		r = r[n:]
	}
	if hasPassword {
		p, n, err := readBinary(r)
		if err != nil {
			return ci, fmt.Errorf("connect: password: %w", err)
		}
		ci.password = string(p)
		_ = n
	}
	return ci, nil
}

func readPrefixedString(b []byte) (string, int, error) {
	if len(b) < 2 {
		return "", 0, errors.New("truncated string length")
	}
	l := int(binary.BigEndian.Uint16(b[:2]))
	if len(b) < 2+l {
		return "", 0, errors.New("truncated string")
	}
	return string(b[2 : 2+l]), 2 + l, nil
}

func readBinary(b []byte) ([]byte, int, error) {
	if len(b) < 2 {
		return nil, 0, errors.New("truncated binary length")
	}
	l := int(binary.BigEndian.Uint16(b[:2]))
	if len(b) < 2+l {
		return nil, 0, errors.New("truncated binary")
	}
	return b[2 : 2+l], 2 + l, nil
}

// parseConnectAuto detects the protocol level and dispatches to the v4 or v5 parser.
func parseConnectAuto(payload []byte) (connectInfo, error) {
	_, n, err := readPrefixedString(payload)
	if err != nil {
		return connectInfo{}, err
	}
	if len(payload) < n+1 {
		return connectInfo{}, errors.New("connect: truncated level")
	}
	if payload[n] == 5 {
		return parseConnectV5(payload)
	}
	return parseConnect(payload)
}
