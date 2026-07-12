package lb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNodeGenerationsApplyIfCurrent(t *testing.T) {
	gens := newNodeGenerations()

	gen := gens.Bump("1", "n1", nil)
	applied := gens.ApplyIfCurrent("1", "n1", gen, func() {})
	assert.True(t, applied)

	// A newer event invalidates the captured generation.
	gens.Bump("1", "n1", nil)
	applied = gens.ApplyIfCurrent("1", "n1", gen, func() {})
	assert.False(t, applied)
}

// Regression: a DELETE arriving while a health check is in flight must
// prevent the check from re-adding the removed node.
func TestNodeGenerationsDeleteDuringHealthCheck(t *testing.T) {
	gens := newNodeGenerations()
	pool := map[string]bool{}

	// PUT starts an async health check with this generation.
	gen := gens.Bump("1", "n1", nil)

	// DELETE applies its removal atomically with the invalidation.
	gens.Bump("1", "n1", func() {
		delete(pool, "n1")
	})

	// The stale health check completes and must be discarded.
	applied := gens.ApplyIfCurrent("1", "n1", gen, func() {
		pool["n1"] = true
	})
	assert.False(t, applied)
	assert.Empty(t, pool)
}

func TestNodeGenerationsIsolatedPerNode(t *testing.T) {
	gens := newNodeGenerations()
	gen1 := gens.Bump("1", "n1", nil)
	gens.Bump("1", "n2", nil)
	gens.Bump("2", "n1", nil)

	// Events for other nodes/chains must not invalidate n1 on chain 1.
	assert.True(t, gens.ApplyIfCurrent("1", "n1", gen1, func() {}))
}
