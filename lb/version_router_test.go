package lb

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChainVersionRouter_Update(t *testing.T) {
	tests := []struct {
		name            string
		chain           string
		version         string
		expectedResolve string
	}{
		{
			name:            "normal version update",
			chain:           "1",
			version:         "a1b2c3d4",
			expectedResolve: "1-a1b2c3d4",
		},
		{
			name:            "version with slash should be normalized",
			chain:           "1",
			version:         "v1/beta",
			expectedResolve: "1-v1-beta",
		},
		{
			name:            "empty version should clear override",
			chain:           "1",
			version:         "",
			expectedResolve: "1", // falls back to original
		},
		{
			name:            "whitespace version should clear override",
			chain:           "1",
			version:         "   ",
			expectedResolve: "1", // falls back to original
		},
		{
			name:            "chain with whitespace should be trimmed",
			chain:           "  1  ",
			version:         "v1",
			expectedResolve: "1-v1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := NewChainVersionRouter()
			router.Update(tt.chain, tt.version)

			trimmedChain := "1"
			if tt.chain != "" {
				trimmedChain = tt.chain
			}
			result := router.Resolve(trimmedChain)
			assert.Equal(t, tt.expectedResolve, result)
		})
	}
}

func TestChainVersionRouter_Update_EmptyChain(t *testing.T) {
	router := NewChainVersionRouter()
	router.Update("", "v1")

	// Empty chain should not be stored
	result := router.Resolve("")
	assert.Equal(t, "", result)
}

func TestChainVersionRouter_Remove(t *testing.T) {
	router := NewChainVersionRouter()

	// Add an override
	router.Update("1", "a1b2c3d4")
	assert.Equal(t, "1-a1b2c3d4", router.Resolve("1"))

	// Remove the override
	router.Remove("1")
	assert.Equal(t, "1", router.Resolve("1"))
}

func TestChainVersionRouter_Remove_NonExistent(t *testing.T) {
	router := NewChainVersionRouter()

	// Remove non-existent should not panic
	router.Remove("non-existent")
	assert.Equal(t, "non-existent", router.Resolve("non-existent"))
}

func TestChainVersionRouter_Remove_EmptyChain(t *testing.T) {
	router := NewChainVersionRouter()

	// Remove empty chain should not panic
	router.Remove("")
	router.Remove("   ")
}

func TestChainVersionRouter_Resolve(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(*ChainVersionRouter)
		chain    string
		expected string
	}{
		{
			name:     "resolve without override returns original",
			setup:    func(r *ChainVersionRouter) {},
			chain:    "1",
			expected: "1",
		},
		{
			name: "resolve with override returns target",
			setup: func(r *ChainVersionRouter) {
				r.Update("1", "a1b2c3d4")
			},
			chain:    "1",
			expected: "1-a1b2c3d4",
		},
		{
			name:     "resolve empty chain returns empty",
			setup:    func(r *ChainVersionRouter) {},
			chain:    "",
			expected: "",
		},
		{
			name:     "resolve whitespace chain returns empty",
			setup:    func(r *ChainVersionRouter) {},
			chain:    "   ",
			expected: "",
		},
		{
			name: "resolve chain with whitespace is trimmed",
			setup: func(r *ChainVersionRouter) {
				r.Update("1", "v1")
			},
			chain:    "  1  ",
			expected: "1-v1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := NewChainVersionRouter()
			tt.setup(router)

			result := router.Resolve(tt.chain)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestChainVersionRouter_MultipleChains(t *testing.T) {
	router := NewChainVersionRouter()

	// Setup multiple chains
	router.Update("1", "a1b2c3d4")
	router.Update("56", "bsc-v2")
	router.Update("137", "polygon-mainnet")

	// Verify each chain resolves correctly
	assert.Equal(t, "1-a1b2c3d4", router.Resolve("1"))
	assert.Equal(t, "56-bsc-v2", router.Resolve("56"))
	assert.Equal(t, "137-polygon-mainnet", router.Resolve("137"))

	// Non-overridden chain should return original
	assert.Equal(t, "100", router.Resolve("100"))
}

func TestChainVersionRouter_UpdateOverwrite(t *testing.T) {
	router := NewChainVersionRouter()

	// Initial update
	router.Update("1", "v1")
	assert.Equal(t, "1-v1", router.Resolve("1"))

	// Overwrite with new version
	router.Update("1", "v2")
	assert.Equal(t, "1-v2", router.Resolve("1"))
}

func TestChainVersionRouter_Concurrency(t *testing.T) {
	router := NewChainVersionRouter()
	var wg sync.WaitGroup

	// Concurrent writes
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			router.Update("1", "v1")
			router.Resolve("1")
			router.Remove("1")
		}(i)
	}

	wg.Wait()
	// Should not panic and should be in a consistent state
	result := router.Resolve("1")
	assert.Equal(t, "1", result) // All removes should have cleared it
}

func TestNewChainVersionRouter(t *testing.T) {
	router := NewChainVersionRouter()
	require.NotNil(t, router)
	require.NotNil(t, router.overrides)
	assert.Empty(t, router.overrides)
}
