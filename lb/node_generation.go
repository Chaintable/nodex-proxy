package lb

import "sync"

// nodeGenerations tracks a monotonically increasing generation per
// (chainId, nodeKey). Discovery events bump the generation; an asynchronous
// health check captures it before starting and re-validates it before
// publishing its result, so a check that raced with a newer PUT/DELETE can
// never resurrect a removed node or overwrite newer state.
type nodeGenerations struct {
	mu   sync.Mutex
	gens map[string]uint64
}

func newNodeGenerations() *nodeGenerations {
	return &nodeGenerations{gens: make(map[string]uint64)}
}

func nodeGenKey(chainId, nodeKey string) string {
	return chainId + "/" + nodeKey
}

// Bump invalidates all in-flight health checks for the node and returns the
// new generation. apply (if non-nil) runs under the same lock, so a
// discovery-side state change is atomic with the invalidation.
func (n *nodeGenerations) Bump(chainId, nodeKey string, apply func()) uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	key := nodeGenKey(chainId, nodeKey)
	n.gens[key]++
	if apply != nil {
		apply()
	}
	return n.gens[key]
}

// ApplyIfCurrent runs apply only if gen is still the node's latest
// generation, holding the lock so no Bump can interleave with apply.
func (n *nodeGenerations) ApplyIfCurrent(chainId, nodeKey string, gen uint64, apply func()) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.gens[nodeGenKey(chainId, nodeKey)] != gen {
		return false
	}
	apply()
	return true
}
