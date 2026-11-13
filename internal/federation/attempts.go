package federation

// Attempt re-keying for active-active queue federation (ADR-010, slice 5).
//
// __queue_attempts keys are `queue\x00partition\x00offset-ascii` — an offset
// in the *message* log, which differs across clusters in active-active mode.
// The mirror therefore re-keys every incoming attempt record into the LOCAL
// offset space via the foreign-applied ranges recorded for the queue topic:
// Local = mapped(Source) when the message has been imported here; otherwise
// the record is dropped from this tick (the source will re-send it next
// round and it will map once the message has landed locally).
//
// The attempts log itself is re-origined on apply: local offsets are fresh,
// no fork check — delta is irrelevant to correctness of re-origin.

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/rahulkj/firn/internal/log"
)

// parseAttemptKey decodes `queue\x00partition\x00offset-ascii`.
// ok=false for malformed keys (they cannot be applied and are dropped).
func parseAttemptKey(key []byte) (queue string, partition int32, offset int64, ok bool) {
	parts := bytes.SplitN(key, []byte{0}, 3)
	if len(parts) != 3 {
		return "", 0, 0, false
	}
	q := string(parts[0])
	p, err := strconv.ParseInt(string(parts[1]), 10, 32)
	if err != nil {
		return "", 0, 0, false
	}
	o, err := strconv.ParseInt(string(parts[2]), 10, 64)
	if err != nil {
		return "", 0, 0, false
	}
	return q, int32(p), o, true
}

// encodeAttemptKey re-encodes a key for the local attempts log.
func encodeAttemptKey(queue string, partition int32, offset int64) []byte {
	return []byte(fmt.Sprintf("%s\x00%d\x00%d", queue, partition, offset))
}

// attemptKeyInScope reports whether a queue is in scope for the peer's
// selectors (empty selector anywhere means all queues).
func attemptKeyInScope(peer Peer, queue string) bool {
	for _, sel := range peer.Selectors {
		if len(sel.Topics) == 0 && len(sel.Queues) == 0 {
			return true
		}
		for _, q := range sel.Queues {
			if q == queue {
				return true
			}
		}
		for _, t := range sel.Topics {
			if t == queue {
				return true
			}
		}
	}
	return false
}

// translateOffsetInverse maps a LOCAL offset (our side) back into the
// source's offset space using the peer's published foreign ranges. Used
// when this node never imported the record from that peer — the record
// exists only in the peer's log.
func translateOffsetInverse(rs []LocalRange, localOffset int64) (int64, bool) {
	for _, r := range rs {
		if localOffset >= r.Start && localOffset <= r.End {
			return r.SourceStart + (localOffset - r.Start), true
		}
	}
	return 0, false
}

// applyRekeyedAttempts applies re-keyed attempt frames to the attempts log.
// frames must carry: Key = re-keyed frame with LOCAL offset semantics in the
// message topic (NOT the attempts log), Value = u32 BE count, Ts preserved.
// The attempts log assigns fresh local offsets on apply.
func applyRekeyedAttempts(fl *log.FileLog, frames []log.Frame) (firstLocal, hwm int64) {
	first := fl.HighWatermark()
	for _, f := range frames {
		fl.AppendReplicated(f.Ts, f.Key, f.Value)
	}
	return first, fl.HighWatermark()
}
