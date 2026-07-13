package etcd

import (
	"context"
	"sync"
	"testing"

	"github.com/Chaintable/nodex-proxy/discovery"
	"github.com/Chaintable/nodex-proxy/lib/log"
	"github.com/stretchr/testify/assert"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
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

// ---- fakes for watch/resync tests ----

type fakeKV struct {
	clientv3.KV
	resp  *clientv3.GetResponse
	err   error
	calls int
}

func (f *fakeKV) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	f.calls++
	return f.resp, f.err
}

// fakeWatcher records the start revision of each Watch call and hands out
// the prepared channels in order; once they are exhausted it closes quit so
// watchConfig exits.
type fakeWatcher struct {
	clientv3.Watcher
	d     *Discover
	once  sync.Once
	revs  []int64
	chans []clientv3.WatchChan
}

func (f *fakeWatcher) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	op := clientv3.OpGet(key, opts...)
	f.revs = append(f.revs, op.Rev())
	i := len(f.revs) - 1
	if i >= len(f.chans) {
		f.once.Do(func() { close(f.d.quit) })
		return make(chan clientv3.WatchResponse)
	}
	return f.chans[i]
}

// A transient watch error must resume from lastProcessed+1 (etcd replays
// missed events) and must NOT consult the KV to jump to a newer revision.
func TestWatchResumesFromLastProcessedPlusOne(t *testing.T) {
	r := newTestDiscover()
	r.watchRevision = 10

	ch1 := make(chan clientv3.WatchResponse, 1)
	ch1 <- clientv3.WatchResponse{
		Header: pb.ResponseHeader{Revision: 12},
		Events: []*clientv3.Event{{
			Type: clientv3.EventTypePut,
			Kv:   &mvccpb.KeyValue{Key: []byte("1/nodes/n1"), Value: []byte(`{"address":"10.0.0.1","port":8545,"nodeType":1,"stateType":1}`)},
		}},
	}
	close(ch1) // transient disconnect after one delivered batch

	kv := &fakeKV{}
	fw := &fakeWatcher{d: r, chans: []clientv3.WatchChan{ch1}}
	r.kv = kv
	r.watcher = fw

	r.watchConfig(context.Background())

	// First watch from 11; after processing the batch at revision 12 the
	// reconnect must start at 13.
	assert.Equal(t, []int64{11, 13}, fw.revs)
	assert.Equal(t, 0, kv.calls, "plain disconnects must not trigger a snapshot Get")

	node := <-r.nodeChannel
	assert.Equal(t, "n1", node.NodeKey)
	assert.Equal(t, EVENT_PUT, node.ChangeType)
}

// Compaction cannot be replayed: it must trigger a full snapshot resync that
// synthesizes DELETEs for vanished keys, re-dispatches changed keys, skips
// unchanged keys, and resumes watching from snapshotRevision+1.
func TestWatchResyncAfterCompaction(t *testing.T) {
	r := newTestDiscover()
	r.watchRevision = 10
	unchanged := []byte(`{"address":"10.0.0.1","port":8545,"nodeType":2,"stateType":1}`)
	r.knownKeys["1/nodes/n1"] = unchanged
	r.knownKeys["1/nodes/n2"] = []byte(`{"address":"10.0.0.2","port":8545,"nodeType":1,"stateType":1}`)

	ch1 := make(chan clientv3.WatchResponse, 1)
	ch1 <- clientv3.WatchResponse{CompactRevision: 42}

	// Snapshot after the outage: n1 unchanged, n2 vanished, n3 new.
	kv := &fakeKV{resp: &clientv3.GetResponse{
		Header: &pb.ResponseHeader{Revision: 50},
		Kvs: []*mvccpb.KeyValue{
			{Key: []byte("1/nodes/n1"), Value: unchanged},
			{Key: []byte("1/nodes/n3"), Value: []byte(`{"address":"10.0.0.3","port":8545,"nodeType":1,"stateType":1}`)},
		},
	}}
	fw := &fakeWatcher{d: r, chans: []clientv3.WatchChan{ch1}}
	r.kv = kv
	r.watcher = fw

	r.watchConfig(context.Background())

	assert.Equal(t, []int64{11, 51}, fw.revs, "must resume from snapshotRevision+1 after resync")
	assert.Equal(t, 1, kv.calls)

	got := map[string]int{}
	for len(r.nodeChannel) > 0 {
		n := <-r.nodeChannel
		got[n.NodeKey] = n.ChangeType
	}
	assert.Equal(t, map[string]int{"n2": EVENT_DELETE, "n3": EVENT_PUT}, got,
		"n2 deleted, n3 added, unchanged n1 not re-dispatched")
	assert.NotContains(t, r.knownKeys, "1/nodes/n2")
	assert.Contains(t, r.knownKeys, "1/nodes/n3")
}

// A failed resync Get must keep the old revision and retry instead of
// losing state.
func TestWatchResyncGetFailureKeepsRevision(t *testing.T) {
	r := newTestDiscover()
	r.watchRevision = 10

	ch1 := make(chan clientv3.WatchResponse, 1)
	ch1 <- clientv3.WatchResponse{CompactRevision: 42}
	kv := &fakeKV{err: assert.AnError}
	fw := &fakeWatcher{d: r, chans: []clientv3.WatchChan{ch1}}
	r.kv = kv
	r.watcher = fw

	r.watchConfig(context.Background())

	// Resync failed, so the next watch still resumes from the old revision.
	assert.Equal(t, []int64{11, 11}, fw.revs)
	assert.Equal(t, int64(10), r.watchRevision)
}
