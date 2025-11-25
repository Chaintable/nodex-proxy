package lb

import (
	"fmt"
	"strings"
	"sync"
)

// ChainVersionRouter keeps track of official version redirections for base chains.
type ChainVersionRouter struct {
	mu        sync.RWMutex
	overrides map[string]string
}

func NewChainVersionRouter() *ChainVersionRouter {
	return &ChainVersionRouter{
		overrides: make(map[string]string),
	}
}

// Update stores or clears a version override for the supplied chain id.
func (r *ChainVersionRouter) Update(chain, version string) {
	chain = strings.TrimSpace(chain)
	version = strings.TrimSpace(version)
	if chain == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if version == "" {
		delete(r.overrides, chain)
		return
	}

	normalizedVersion := strings.ReplaceAll(version, "/", "-")
	r.overrides[chain] = fmt.Sprintf("%s-%s", chain, normalizedVersion)
}

// Remove deletes any override for the supplied chain id.
func (r *ChainVersionRouter) Remove(chain string) {
	chain = strings.TrimSpace(chain)
	if chain == "" {
		return
	}

	r.mu.Lock()
	delete(r.overrides, chain)
	r.mu.Unlock()
}

// Resolve returns the effective chain id, falling back to the original when
// no override exists.
func (r *ChainVersionRouter) Resolve(chain string) string {
	chain = strings.TrimSpace(chain)
	if chain == "" {
		return ""
	}

	r.mu.RLock()
	target, ok := r.overrides[chain]
	r.mu.RUnlock()
	if !ok {
		return chain
	}
	return target
}
