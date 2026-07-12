package etcd

import (
	"testing"

	"github.com/Chaintable/nodex-proxy/discovery"
	"github.com/Chaintable/nodex-proxy/lib/log"
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
			input:    "1/a1b2c3d4",
			expected: "1-a1b2c3d4",
		},
		{
			name:     "chain id with multiple slashes",
			input:    "1/v1/beta",
			expected: "1-v1-beta",
		},
		{
			name:     "chain id with leading slash",
			input:    "/1/a1b2c3d4",
			expected: "1-a1b2c3d4",
		},
		{
			name:     "chain id with trailing slash",
			input:    "1/a1b2c3d4/",
			expected: "1-a1b2c3d4",
		},
		{
			name:     "chain id with leading and trailing slashes",
			input:    "/1/a1b2c3d4/",
			expected: "1-a1b2c3d4",
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
			input:    []byte(`"a1b2c3d4"`),
			expected: "a1b2c3d4",
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
			{"1/a1b2c3d4/nodes/127.0.0.1_8545", true, "1/a1b2c3d4", "127.0.0.1_8545"},
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

	t.Run("nativeNodesPattern", func(t *testing.T) {
		tests := []struct {
			input         string
			shouldMatch   bool
			expectedChain string
			expectedNode  string
		}{
			{"1/nativeNodes/127.0.0.1_8545", true, "1", "127.0.0.1_8545"},
			{"1/a1b2c3d4/nativeNodes/127.0.0.1_8545", true, "1/a1b2c3d4", "127.0.0.1_8545"},
			{"56/nativeNodes/172.21.59.215_8771", true, "56", "172.21.59.215_8771"},
			{"1/lastBlockNumber", false, "", ""},
			{"1/nodes/127.0.0.1_8545", false, "", ""},
			{"1/gateway", false, "", ""},
		}

		for _, tt := range tests {
			match := nativeNodesPattern.FindStringSubmatch(tt.input)
			if tt.shouldMatch {
				assert.NotNil(t, match, "expected match for %s", tt.input)
				assert.Equal(t, tt.expectedChain, match[nativeNodesPattern.SubexpIndex("chain")])
				assert.Equal(t, tt.expectedNode, match[nativeNodesPattern.SubexpIndex("node")])
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
			{"1/a1b2c3d4/lastBlockNumber", true, "1/a1b2c3d4"},
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
			{"1/a1b2c3d4/version", true, "1/a1b2c3d4"},
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
			{"1/a1b2c3d4/gateway", true, "1/a1b2c3d4"},
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
			{"1/a1b2c3d4/mirror/0xabc", true, "1/a1b2c3d4", "0xabc"},
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
			key:           "1/a1b2c3d4/nodes/127.0.0.1_8545",
			expectedChain: "1/a1b2c3d4",
			normalizedID:  "1-a1b2c3d4",
		},
		{
			name:          "versioned chain native node",
			key:           "1/a1b2c3d4/nativeNodes/127.0.0.1_8545",
			expectedChain: "1/a1b2c3d4",
			normalizedID:  "1-a1b2c3d4",
		},
		{
			name:          "versioned chain height",
			key:           "1/e5f6g7h8/lastBlockNumber",
			expectedChain: "1/e5f6g7h8",
			normalizedID:  "1-e5f6g7h8",
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
			} else if match := nativeNodesPattern.FindStringSubmatch(tt.key); match != nil {
				chainFromPattern = match[nativeNodesPattern.SubexpIndex("chain")]
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

func newTestDiscover() *Discover {
	log.InitLogger("error")
	return &Discover{
		quit:           make(chan struct{}),
		keyPrefix:      "",
		knownKeys:      make(map[string][]byte),
		nodeChannel:    make(chan *discovery.TargetNode, 16),
		heightChan:     make(chan *discovery.ChainHeight, 16),
		gatewayChannel: make(chan *discovery.Gateway, 16),
		mirrorChannel:  make(chan *discovery.MirrorTarget, 16),
		versionChannel: make(chan *discovery.ChainVersion, 16),
	}
}

func TestDispatchPutRecordsKnownKeys(t *testing.T) {
	r := newTestDiscover()

	r.dispatchPut("1/nodes/n1", []byte(`{"address":"10.0.0.1","port":8545,"nodeType":2,"stateType":1}`))

	node := <-r.nodeChannel
	assert.Equal(t, "1", node.ChainId)
	assert.Equal(t, "n1", node.NodeKey)
	assert.Equal(t, EVENT_PUT, node.ChangeType)
	assert.Contains(t, r.knownKeys, "1/nodes/n1")

	// Unparseable payloads are not delivered and must not be tracked.
	r.dispatchPut("1/nodes/bad", []byte(`{not json`))
	assert.NotContains(t, r.knownKeys, "1/nodes/bad")
	assert.Empty(t, r.nodeChannel)
}

// A DELETE without PrevKV must fall back to the last value seen for the key
// so consumers still learn the node's type and source.
func TestDispatchDeletePrevValueFallback(t *testing.T) {
	r := newTestDiscover()

	r.dispatchPut("1/nodes/n1", []byte(`{"address":"10.0.0.1","port":8545,"nodeType":2,"stateType":1}`))
	<-r.nodeChannel

	r.dispatchDelete("1/nodes/n1", nil)

	node := <-r.nodeChannel
	assert.Equal(t, EVENT_DELETE, node.ChangeType)
	assert.Equal(t, discovery.NodeType(2), node.NodeType)
	assert.Equal(t, "10.0.0.1", node.Address)
	assert.NotContains(t, r.knownKeys, "1/nodes/n1")
}

func TestDispatchDeleteNativeNode(t *testing.T) {
	r := newTestDiscover()

	r.dispatchPut("1/nativeNodes/n1", []byte(`{"address":"10.0.0.2","port":8545,"nodeType":1,"stateType":1}`))
	put := <-r.nodeChannel
	assert.Equal(t, "native", put.Source)

	r.dispatchDelete("1/nativeNodes/n1", nil)
	del := <-r.nodeChannel
	assert.Equal(t, EVENT_DELETE, del.ChangeType)
	assert.Equal(t, "native", del.Source)
}
