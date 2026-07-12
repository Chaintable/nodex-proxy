package lb

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNodeGenerationsApplyIfCurrent(t *testing.T) {
	gens := newNodeGenerations()
	id := nodeIdentity{chainId: "1", nodeKey: "n1"}

	gen := gens.Bump(id)
	assert.True(t, gens.ApplyIfCurrent(id, gen, func() {}))

	// A newer event invalidates the captured generation.
	gens.Bump(id)
	assert.False(t, gens.ApplyIfCurrent(id, gen, func() {}))
}

// Regression: a DELETE arriving while a health check is in flight must
// prevent the check from re-adding the removed node, even if the removal
// itself happens outside the generation lock.
func TestNodeGenerationsDeleteDuringHealthCheck(t *testing.T) {
	gens := newNodeGenerations()
	id := nodeIdentity{chainId: "1", nodeKey: "n1"}
	pool := map[string]bool{}

	// PUT starts an async health check with this generation.
	gen := gens.Bump(id)

	// DELETE invalidates first, then removes.
	gens.Forget(id)
	delete(pool, "n1")

	// The stale health check completes and must be discarded.
	assert.False(t, gens.ApplyIfCurrent(id, gen, func() {
		pool["n1"] = true
	}))
	assert.Empty(t, pool)
}

// Regression: <chain>/nodes/<key> and <chain>/nativeNodes/<key> are separate
// namespaces; a same-named native node must not invalidate the regular
// node's in-flight health check, and vice versa.
func TestNodeGenerationsRegularAndNativeSameKey(t *testing.T) {
	gens := newNodeGenerations()
	regular := nodeIdentity{chainId: "1", nodeKey: "n1", native: false}
	native := nodeIdentity{chainId: "1", nodeKey: "n1", native: true}

	regularGen := gens.Bump(regular)
	nativeGen := gens.Bump(native)

	assert.True(t, gens.ApplyIfCurrent(regular, regularGen, func() {}))
	assert.True(t, gens.ApplyIfCurrent(native, nativeGen, func() {}))

	// Deleting the native node must not affect the regular node.
	nextRegularGen := gens.Bump(regular)
	gens.Forget(native)
	assert.True(t, gens.ApplyIfCurrent(regular, nextRegularGen, func() {}))
	assert.False(t, gens.ApplyIfCurrent(native, nativeGen, func() {}))
}

// No ABA: after Forget, a new Bump draws from the global sequence, so a
// check captured before the delete can never match the re-added node.
func TestNodeGenerationsNoABAAfterForget(t *testing.T) {
	gens := newNodeGenerations()
	id := nodeIdentity{chainId: "1", nodeKey: "n1"}

	staleGen := gens.Bump(id) // health check in flight
	gens.Forget(id)           // DELETE
	freshGen := gens.Bump(id) // node re-added with the same key

	assert.False(t, gens.ApplyIfCurrent(id, staleGen, func() {}))
	assert.True(t, gens.ApplyIfCurrent(id, freshGen, func() {}))
	assert.Empty(t, gens.gens[nodeIdentity{}]) // sanity: no zero-id entry
}

func TestNodeGenerationsIsolatedPerNode(t *testing.T) {
	gens := newNodeGenerations()
	gen1 := gens.Bump(nodeIdentity{chainId: "1", nodeKey: "n1"})
	gens.Bump(nodeIdentity{chainId: "1", nodeKey: "n2"})
	gens.Bump(nodeIdentity{chainId: "2", nodeKey: "n1"})

	// Events for other nodes/chains must not invalidate n1 on chain 1.
	assert.True(t, gens.ApplyIfCurrent(nodeIdentity{chainId: "1", nodeKey: "n1"}, gen1, func() {}))
}

// Concurrent checks and deletes must converge: after a Forget wins the race,
// no stale apply may run. Run with -race.
func TestNodeGenerationsConcurrent(t *testing.T) {
	gens := newNodeGenerations()
	id := nodeIdentity{chainId: "1", nodeKey: "n1"}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		gen := gens.Bump(id)
		wg.Add(2)
		go func(gen uint64) {
			defer wg.Done()
			gens.ApplyIfCurrent(id, gen, func() {})
		}(gen)
		go func() {
			defer wg.Done()
			gens.Forget(id)
		}()
	}
	wg.Wait()

	gens.Forget(id)
	// After the final Forget nothing can apply.
	assert.False(t, gens.ApplyIfCurrent(id, 1, func() {}))
	assert.Len(t, gens.gens, 0)
}
