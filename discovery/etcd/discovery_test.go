package etcd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNormalizeMultiVersionChainID(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple chain id",
			input:    "1",
			expected: "1",
		},
		{
			name:     "chain id with version",
			input:    "1/multivesion1",
			expected: "1-multivesion1",
		},
		{
			name:     "chain id with multiple slashes",
			input:    "1/v1/beta",
			expected: "1-v1-beta",
		},
		{
			name:     "chain id with leading slash",
			input:    "/1/multivesion1",
			expected: "1-multivesion1",
		},
		{
			name:     "chain id with trailing slash",
			input:    "1/multivesion1/",
			expected: "1-multivesion1",
		},
		{
			name:     "chain id with leading and trailing slashes",
			input:    "/1/multivesion1/",
			expected: "1-multivesion1",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "only slashes",
			input:    "///",
			expected: "",
		},
		{
			name:     "chain id without version",
			input:    "56",
			expected: "56",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizeMultiVersionChainID(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseVersionValue(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected string
	}{
		{
			name:     "json object with version field",
			input:    []byte(`{"version": "v1.0.0"}`),
			expected: "v1.0.0",
		},
		{
			name:     "json object with version field and extra fields",
			input:    []byte(`{"version": "v2.0.0", "other": "data"}`),
			expected: "v2.0.0",
		},
		{
			name:     "json string",
			input:    []byte(`"v1.0.0"`),
			expected: "v1.0.0",
		},
		{
			name:     "plain text",
			input:    []byte(`v1.0.0`),
			expected: "v1.0.0",
		},
		{
			name:     "plain text with quotes",
			input:    []byte(`"multivesion1"`),
			expected: "multivesion1",
		},
		{
			name:     "empty byte slice",
			input:    []byte{},
			expected: "",
		},
		{
			name:     "nil byte slice",
			input:    nil,
			expected: "",
		},
		{
			name:     "whitespace only",
			input:    []byte("   "),
			expected: "",
		},
		{
			name:     "json object with empty version",
			input:    []byte(`{"version": ""}`),
			expected: `{"version": ""}`, // falls back to trimmed string when version is empty
		},
		{
			name:     "json object with whitespace version",
			input:    []byte(`{"version": "   "}`),
			expected: `{"version": "   "}`, // falls back to trimmed string when version is whitespace
		},
		{
			name:     "version with leading/trailing whitespace",
			input:    []byte(`{"version": "  v1.0.0  "}`),
			expected: "v1.0.0",
		},
		{
			name:     "json string with whitespace",
			input:    []byte(`"  v1.0.0  "`),
			expected: "v1.0.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseVersionValue(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPatternMatching(t *testing.T) {
	t.Run("nodesPattern", func(t *testing.T) {
		tests := []struct {
			input         string
			shouldMatch   bool
			expectedChain string
			expectedNode  string
		}{
			{"1/nodes/127.0.0.1_8545", true, "1", "127.0.0.1_8545"},
			{"1/multivesion1/nodes/127.0.0.1_8545", true, "1/multivesion1", "127.0.0.1_8545"},
			{"56/nodes/172.21.59.215_8771", true, "56", "172.21.59.215_8771"},
			{"1/lastBlockNumber", false, "", ""},
			{"1/gateway", false, "", ""},
		}

		for _, tt := range tests {
			match := nodesPattern.FindStringSubmatch(tt.input)
			if tt.shouldMatch {
				assert.NotNil(t, match, "expected match for %s", tt.input)
				assert.Equal(t, tt.expectedChain, match[nodesPattern.SubexpIndex("chain")])
				assert.Equal(t, tt.expectedNode, match[nodesPattern.SubexpIndex("node")])
			} else {
				assert.Nil(t, match, "expected no match for %s", tt.input)
			}
		}
	})

	t.Run("lastBlockPattern", func(t *testing.T) {
		tests := []struct {
			input         string
			shouldMatch   bool
			expectedChain string
		}{
			{"1/lastBlockNumber", true, "1"},
			{"1/multivesion1/lastBlockNumber", true, "1/multivesion1"},
			{"56/lastBlockNumber", true, "56"},
			{"1/nodes/127.0.0.1_8545", false, ""},
		}

		for _, tt := range tests {
			match := lastBlockPattern.FindStringSubmatch(tt.input)
			if tt.shouldMatch {
				assert.NotNil(t, match, "expected match for %s", tt.input)
				assert.Equal(t, tt.expectedChain, match[lastBlockPattern.SubexpIndex("chain")])
			} else {
				assert.Nil(t, match, "expected no match for %s", tt.input)
			}
		}
	})

	t.Run("versionPattern", func(t *testing.T) {
		tests := []struct {
			input         string
			shouldMatch   bool
			expectedChain string
		}{
			{"1/version", true, "1"},
			{"1/multivesion1/version", true, "1/multivesion1"},
			{"56/version", true, "56"},
			{"1/nodes/127.0.0.1_8545", false, ""},
			{"1/lastBlockNumber", false, ""},
		}

		for _, tt := range tests {
			match := versionPattern.FindStringSubmatch(tt.input)
			if tt.shouldMatch {
				assert.NotNil(t, match, "expected match for %s", tt.input)
				assert.Equal(t, tt.expectedChain, match[versionPattern.SubexpIndex("chain")])
			} else {
				assert.Nil(t, match, "expected no match for %s", tt.input)
			}
		}
	})

	t.Run("gateWayPattern", func(t *testing.T) {
		tests := []struct {
			input         string
			shouldMatch   bool
			expectedChain string
		}{
			{"1/gateway", true, "1"},
			{"1/multivesion1/gateway", true, "1/multivesion1"},
			{"56/gateway", true, "56"},
			{"1/nodes/127.0.0.1_8545", false, ""},
		}

		for _, tt := range tests {
			match := gateWayPattern.FindStringSubmatch(tt.input)
			if tt.shouldMatch {
				assert.NotNil(t, match, "expected match for %s", tt.input)
				assert.Equal(t, tt.expectedChain, match[gateWayPattern.SubexpIndex("chain")])
			} else {
				assert.Nil(t, match, "expected no match for %s", tt.input)
			}
		}
	})

	t.Run("mirrorPattern", func(t *testing.T) {
		tests := []struct {
			input         string
			shouldMatch   bool
			expectedChain string
			expectedAddr  string
		}{
			{"1/mirror/0x123", true, "1", "0x123"},
			{"1/multivesion1/mirror/0xabc", true, "1/multivesion1", "0xabc"},
			{"1/nodes/127.0.0.1_8545", false, "", ""},
		}

		for _, tt := range tests {
			match := mirrorPattern.FindStringSubmatch(tt.input)
			if tt.shouldMatch {
				assert.NotNil(t, match, "expected match for %s", tt.input)
				assert.Equal(t, tt.expectedChain, match[mirrorPattern.SubexpIndex("chain")])
				assert.Equal(t, tt.expectedAddr, match[mirrorPattern.SubexpIndex("addr")])
			} else {
				assert.Nil(t, match, "expected no match for %s", tt.input)
			}
		}
	})
}

func TestNormalizeMultiVersionChainID_Integration(t *testing.T) {
	// Test the full flow: pattern match -> normalize
	tests := []struct {
		name          string
		key           string
		expectedChain string
		normalizedID  string
	}{
		{
			name:          "simple chain node",
			key:           "1/nodes/127.0.0.1_8545",
			expectedChain: "1",
			normalizedID:  "1",
		},
		{
			name:          "versioned chain node",
			key:           "1/multivesion1/nodes/127.0.0.1_8545",
			expectedChain: "1/multivesion1",
			normalizedID:  "1-multivesion1",
		},
		{
			name:          "versioned chain height",
			key:           "1/multivesion2/lastBlockNumber",
			expectedChain: "1/multivesion2",
			normalizedID:  "1-multivesion2",
		},
		{
			name:          "versioned chain version key",
			key:           "1/version",
			expectedChain: "1",
			normalizedID:  "1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var chainFromPattern string

			if match := nodesPattern.FindStringSubmatch(tt.key); match != nil {
				chainFromPattern = match[nodesPattern.SubexpIndex("chain")]
			} else if match := lastBlockPattern.FindStringSubmatch(tt.key); match != nil {
				chainFromPattern = match[lastBlockPattern.SubexpIndex("chain")]
			} else if match := versionPattern.FindStringSubmatch(tt.key); match != nil {
				chainFromPattern = match[versionPattern.SubexpIndex("chain")]
			}

			assert.Equal(t, tt.expectedChain, chainFromPattern)

			normalized := normalizeMultiVersionChainID(chainFromPattern)
			assert.Equal(t, tt.normalizedID, normalized)
		})
	}
}
