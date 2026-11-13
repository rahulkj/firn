package replication

// SingleNodeView is a trivial ClusterView for single-node deployments: one
// node, always the leader, returning its own advertised address.
type SingleNodeView struct {
	NodeID string
	Addr   string
}

func (v *SingleNodeView) ID() string             { return v.NodeID }
func (v *SingleNodeView) AdvertisedAddr() string { return v.Addr }
func (v *SingleNodeView) NodeAddr(id string) (string, bool) {
	if id == v.NodeID {
		return v.Addr, true
	}
	return "", false
}
func (v *SingleNodeView) AllNodes() []string { return []string{v.NodeID} }
func (v *SingleNodeView) Leader() string     { return v.NodeID }
