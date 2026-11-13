package quorum

import (
	"fmt"
	"sync"
)

// LocalTransport delivers messages synchronously into the recipient's
// mailbox — usable only for tests. TCP transport lands with phase C.
type LocalTransport struct {
	id string
}

// NewLocalTransport returns a transport with the given address.
func NewLocalTransport(id string) *LocalTransport { return &LocalTransport{id: id} }

// Addr returns the transport's address.
func (t *LocalTransport) Addr() string { return t.id }

var registry sync.Map // addr -> *Node

// Register wires the node into the transport registry.
func RegisterLocalNode(n *Node) {
	registry.Store(n.Addr(), n)
}

// Unregister removes the node from the registry (test helper).
func UnregisterLocalNode(n *Node) {
	registry.Delete(n.Addr())
}

// Send delivers the message to the node registered at `to`.
func (t *LocalTransport) Send(to string, msg Message) error {
	v, ok := registry.Load(to)
	if !ok {
		return fmt.Errorf("slipway/quorum: unknown peer %q", to)
	}
	n := v.(*Node)
	select {
	case n.mailbox <- msg:
	default:
		// Mailbox full: drop the message (leader will retry).
	}
	return nil
}
