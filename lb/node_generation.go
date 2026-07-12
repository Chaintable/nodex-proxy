package lb

import "sync"

// nodeIdentity identifies a node within its selector pool. Regular
// (state/archive) nodes and native nodes are separate namespaces both in
// etcd (<chain>/nodes/<key> vs <chain>/nativeNodes/<key>) and in the
// selector pools, so a same-named native node must not invalidate a regular
// node's in-flight health check or vice versa.
type nodeIdentity struct {
	chainId string
	nodeKey string
	native  bool
}

// nodeGenerations invalidates in-flight health checks that raced with newer
// discovery events. A PUT bumps the node's generation before starting its
// health check; the check re-validates the generation before publishing, so
// a stale result can never resurrect a removed node or overwrite newer
// state. Generations are drawn from one monotonically increasing sequence,
// so Forget can delete entries outright: a later PUT gets a strictly larger
// generation and an old check can never match again (no ABA), keeping the
// map bounded by the number of live nodes.
type nodeGenerations struct {
	mu   sync.Mutex
	seq  uint64
	gens map[nodeIdentity]uint64
}

func newNodeGenerations() *nodeGenerations {
	return &nodeGenerations{gens: make(map[nodeIdentity]uint64)}
}

// Bump registers a new generation for the node, invalidating all in-flight
// health checks for it, and returns the generation to be captured by the
// check started for this event.
func (n *nodeGenerations) Bump(id nodeIdentity) uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.seq++
	n.gens[id] = n.seq
	return n.seq
}

// Forget drops the node's entry, invalidating all in-flight health checks
// for it: generations start at 1, so a missing entry can never match.
func (n *nodeGenerations) Forget(id nodeIdentity) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.gens, id)
}

// ApplyIfCurrent runs apply only if gen is still the node's latest
// generation, holding the lock so no Bump/Forget can interleave with apply.
func (n *nodeGenerations) ApplyIfCurrent(id nodeIdentity, gen uint64, apply func()) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.gens[id] != gen {
		return false
	}
	apply()
	return true
}
