package jsonrpc

import (
	"sync"
	"testing"

	"github.com/Chaintable/nodex-proxy/discovery"
	"github.com/stretchr/testify/assert"
)

func TestGetWeightForChainReturnsCopy(t *testing.T) {
	g := NewGatewayStrategy()
	g.UpdateWeightForChain("1", []discovery.WeightInfo{{NodeKey: "n1", Weight: 10}})

	weights, exists := g.GetWeightForChain("1")
	assert.True(t, exists)
	assert.Equal(t, 10, weights["n1"])

	// Mutating the returned map must not leak into the strategy's state.
	weights["n1"] = 999
	weights["injected"] = 1

	fresh, _ := g.GetWeightForChain("1")
	assert.Equal(t, 10, fresh["n1"])
	_, injected := fresh["injected"]
	assert.False(t, injected)
}

func TestGetWeightForChainMissingChain(t *testing.T) {
	g := NewGatewayStrategy()
	weights, exists := g.GetWeightForChain("nope")
	assert.False(t, exists)
	assert.NotNil(t, weights)
	assert.Empty(t, weights)
}

// Concurrent readers writing their returned map alongside updates used to be
// a fatal concurrent map write; run with -race to guard the copy semantics.
func TestGetWeightForChainConcurrentAccess(t *testing.T) {
	g := NewGatewayStrategy()
	g.UpdateWeightForChain("1", []discovery.WeightInfo{{NodeKey: "n1", Weight: 10}})

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				weights, _ := g.GetWeightForChain("1")
				weights["n2"] = j // callers may fill in defaults locally
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 500; j++ {
			g.UpdateWeightForChain("1", []discovery.WeightInfo{{NodeKey: "n1", Weight: j}})
		}
	}()
	wg.Wait()
}
