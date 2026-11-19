package ingest

// MQTT 5.0 support (CONNECT/CONNACK v5 with properties and reason codes,
// PUBLISH/PUBACK/PUBREC/PUBREL/PUBCOMP with property sections, SUBSCRIBE
// refused with per-filter reason codes, DISCONNECT reason codes). The
// gateway remains publish-only: session state is stateless, subscriptions
// are refused, extended authentication is refused. Anything not implemented
// here fails closed with a valid 5.0 reason code rather than being silently
// accepted.

import (
	"errors"
	"fmt"
)

// connectError carries a v5 reason code for a CONNECT failure CONNACK.
type connectError struct {
	reason byte
	msg    string
}

func (e *connectError) Error() string { return e.msg }

type propertyKind int

const (
	pByte propertyKind = iota // 1 byte
	pShort                    // 2 bytes
	pLong                     // 4 bytes
	pEight                    // 8 bytes
	pString                   // UTF-8 string (2-byte length)
	pBinary                   // binary data (2-byte length)
	pVarint                   // MQTT varint
	pPair                     // two UTF-8 strings (user property only)
)

// propertyKinds maps every property identifier defined by MQTT 5.0 to its
// wire type. Unknown identifiers abort the handshake (fail-closed).
var propertyKinds = map[uint32]propertyKind{
	0x01: pByte,   // Payload Format Indicator
	0x02: pLong,   // Message Expiry Interval
	0x03: pString, // Content Type
	0x08: pString, // Response Topic
	0x09: pBinary, // Correlation Data
	0x0B: pVarint, // Subscription Identifier
	0x11: pLong,   // Session Expiry Interval
	0x12: pString, // Assigned Client Identifier
	0x13: pShort,  // Server Keep Alive
	0x15: pString, // Authentication Method
	0x16: pBinary, // Authentication Data
	0x17: pByte,   // Request Problem Information
	0x18: pLong,   // Will Delay Interval
	0x19: pByte,   // Request Response Information
	0x1A: pString, // Response Information
	0x1C: pString, // Server Reference
	0x1F: pString, // Reason String
	0x21: pShort,  // Receive Maximum
	0x22: pShort,  // Topic Alias Maximum
	0x23: pShort,  // Topic Alias
	0x24: pByte,   // Maximum QoS
	0x25: pByte,   // Retain Available
	0x26: pPair,   // User Property
	0x27: pLong,   // Maximum Packet Size
	0x28: pByte,   // Wildcard Subscription Available
	0x29: pByte,   // Subscription Identifier Available
	0x2A: pByte,   // Shared Subscription Available
}

// property is one parsed property (user properties appear as repeated props).
type property struct {
	id    uint32
	value []byte
}

// parseProperties reads a property section starting at b[0] (the varint
// length prefix) and returns the parsed properties and the remaining bytes.
func parseProperties(b []byte) ([]property, []byte, error) {
	if len(b) == 0 {
		return nil, b, errors.New("mqtt5: missing properties section")
	}
	length := 0
	mult := 1
	consumed := 0
	for i := 0; ; i++ {
		if i >= 4 {
			return nil, b, errors.New("mqtt5: malformed properties length")
		}
		d := b[i]
		length += int(d&0x7f) * mult
		consumed++
		if d&0x80 == 0 {
			break
		}
		mult *= 128
	}
	if len(b) < consumed+length {
		return nil, b, errors.New("mqtt5: truncated properties")
	}
	props := []property{}
	r := b[consumed : consumed+length]
	for len(r) > 0 {
		id, n, err := readVarint(r)
		if err != nil {
			return nil, b, err
		}
		r = r[n:]
		kind, ok := propertyKinds[id]
		if !ok {
			return nil, b, fmt.Errorf("mqtt5: unknown property identifier %d", id)
		}
		var val []byte
		switch kind {
		case pByte:
			if len(r) < 1 {
				return nil, b, fmt.Errorf("mqtt5: property %d truncated", id)
			}
			val = r[:1]
			r = r[1:]
		case pShort:
			if len(r) < 2 {
				return nil, b, fmt.Errorf("mqtt5: property %d truncated", id)
			}
			val = r[:2]
			r = r[2:]
		case pLong:
			if len(r) < 4 {
				return nil, b, fmt.Errorf("mqtt5: property %d truncated", id)
			}
			val = r[:4]
			r = r[4:]
		case pEight:
			if len(r) < 8 {
				return nil, b, fmt.Errorf("mqtt5: property %d truncated", id)
			}
			val = r[:8]
			r = r[8:]
		case pVarint:
			_, vn, err := readVarint(r)
			if err != nil {
				return nil, b, err
			}
			val = r[:vn]
			r = r[vn:]
		case pString, pBinary:
			if len(r) < 2 {
				return nil, b, fmt.Errorf("mqtt5: property %d truncated", id)
			}
			l := int(readUint16(r))
			if len(r) < 2+l {
				return nil, b, fmt.Errorf("mqtt5: property %d truncated", id)
			}
			val = r[:2+l]
			r = r[2+l:]
		case pPair:
			if len(r) < 2 {
				return nil, b, fmt.Errorf("mqtt5: property %d truncated", id)
			}
			l1 := int(readUint16(r))
			if len(r) < 2+l1 {
				return nil, b, fmt.Errorf("mqtt5: property %d truncated", id)
			}
			rest := r[2+l1:]
			if len(rest) < 2 {
				return nil, b, fmt.Errorf("mqtt5: property %d truncated", id)
			}
			l2 := int(readUint16(rest))
			if len(rest) < 2+l2 {
				return nil, b, fmt.Errorf("mqtt5: property %d truncated", id)
			}
			val = r[:2+l1+2+l2]
			r = rest[2+l2:]
		}
		props = append(props, property{id: id, value: val})
	}
	return props, b[consumed+length:], nil
}

func readVarint(b []byte) (uint32, int, error) {
	v := uint32(0)
	mult := uint32(1)
	for i := 0; i < 4 && i < len(b); i++ {
		d := b[i]
		v += uint32(d&0x7f) * mult
		if d&0x80 == 0 {
			return v, i + 1, nil
		}
		mult *= 128
	}
	return 0, 0, errors.New("mqtt5: malformed varint")
}

func readUint16(b []byte) uint16 {
	return uint16(b[0])<<8 | uint16(b[1])
}

func appendVarintBuf(b []byte, v uint32) []byte {
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

// connAckProps builds the property section for a CONNACK. Success carries
// session-expiry=0 and receive-maximum=100; failures carry a reason string.
func connAckProps(reasonCode byte, reason string) []byte {
	out := []byte{}
	switch reasonCode {
	case 0x00:
		out = append(out, 0x11, 0x00, 0x00, 0x00, 0x00) // session expiry = 0
		out = append(out, 0x21, 0x00, 0x64)            // receive maximum = 100
		out = append(out, 0x27, 0x00, 0x04, 0x00, 0x00) // max packet size 256 KiB
	default:
		if reason != "" {
			out = append(out, 0x1F)
			out = append(out, byte(len(reason)>>8), byte(len(reason)))
			out = append(out, reason...)
		}
	}
	body := []byte{}
	body = appendVarintBuf(body, uint32(len(out)))
	body = append(body, out...)
	return body
}

// parseConnectV5 decodes an MQTT 5.0 CONNECT payload.
func parseConnectV5(payload []byte) (connectInfo, error) {
	var ci connectInfo
	r := payload
	_, n, err := readPrefixedString(r) // protocol name "MQTT"
	if err != nil {
		return ci, fmt.Errorf("connect: protocol name: %w", err)
	}
	r = r[n:]
	if len(r) < 4 {
		return ci, errors.New("connect: truncated header")
	}
	if r[0] != 5 {
		return ci, fmt.Errorf("connect: unsupported protocol level %d", r[0])
	}
	ci.version = 5
	connectFlags := r[1]
	r = r[4:] // level + flags + keepalive
	// MQTT 5.0: Properties before payload.
	props, rest, err := parseProperties(r)
	if err != nil {
		return ci, fmt.Errorf("connect: properties: %w", err)
	}
	r = rest
	for _, p := range props {
		switch p.id {
		case 0x15: // authentication method
			ci.authMethod = string(p.value[2:])
		}
	}
	if ci.authMethod != "" {
		return ci, &connectError{reason: 0x86, msg: fmt.Sprintf("connect: extended authentication %q is not supported by this gateway", ci.authMethod)}
	}
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
		// Will properties (v5) then will topic then will payload.
		_, rest, err := parseProperties(r)
		if err != nil {
			return ci, fmt.Errorf("connect: will properties: %w", err)
		}
		r = rest
		_, n, err := readPrefixedString(r) // will topic
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
