package lb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChainSetNormalizesLikeRequests(t *testing.T) {
	set := chainSet([]string{"0x38", " 8453 "})
	for _, raw := range []string{"56", "0x38", "8453"} {
		base, _, _ := splitChainIdentifier(raw)
		require.Contains(t, set, base)
	}
	require.NotContains(t, set, "1")
}
