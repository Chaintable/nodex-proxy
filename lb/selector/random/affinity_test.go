package random

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/Chaintable/nodex-proxy/discovery"
	"github.com/Chaintable/nodex-proxy/lb/jsonrpc"
	"github.com/Chaintable/nodex-proxy/lb/lbnode"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/Chaintable/nodex-proxy/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/stretchr/testify/require"
)

const affinityChain = "56"

func affinityNodes(t *testing.T, n int, nodeType discovery.NodeType, opts ...lbnode.Option) []*lbnode.Node {
	t.Helper()
	nodes := make([]*lbnode.Node, n)
	for i := range nodes {
		node, err := lbnode.New(fmt.Sprintf("172.24.0.%d_8574", i), fmt.Sprintf("172.24.0.%d", i), 8574, 100, nodeType, opts...)
		require.NoError(t, err)
		node.SetState(1)
		nodes[i] = node
	}
	return nodes
}

var affinityOn = map[string]struct{}{affinityChain: {}}

func newAffinitySelector(t *testing.T, gs jsonrpc.GatewayStrategy, blockAffinity bool, nodeType discovery.NodeType, nodes []*lbnode.Node) *Random {
	t.Helper()
	var chains map[string]struct{}
	if blockAffinity {
		chains = affinityOn
	}
	r := New(utils.PickNodes, gs, chains)
	for _, node := range nodes {
		require.NoError(t, r.UpsertNode(context.Background(), affinityChain, nodeType, node))
	}
	return r
}

func hashRequest(hash common.Hash, archive bool) *types.RequestContext {
	return &types.RequestContext{
		ChainId:      affinityChain,
		BaseChainId:  affinityChain,
		Archive:      archive,
		BlockContext: &types.BlockContext{Type: "Equals", BlockId: &rpc.BlockNumberOrHash{BlockHash: &hash}},
	}
}

func randomHash(rng *rand.Rand) common.Hash {
	var h common.Hash
	rng.Read(h[:])
	return h
}

func distinctPicks(t *testing.T, r *Random, req *types.RequestContext) int {
	t.Helper()
	seen := map[string]struct{}{}
	for i := 0; i < 200; i++ {
		node, err := r.GetNode(req, "")
		require.NoError(t, err)
		seen[node.Key()] = struct{}{}
	}
	return len(seen)
}

// Proxy replicas see the pool in different orders but must agree on the node.
func TestBlockAffinitySameNodeAcrossReplicas(t *testing.T) {
	nodes := affinityNodes(t, 22, discovery.NodeTypeArchive)
	reversed := make([]*lbnode.Node, len(nodes))
	for i, node := range nodes {
		reversed[len(nodes)-1-i] = node
	}
	a := newAffinitySelector(t, nil, true, discovery.NodeTypeArchive, nodes)
	b := newAffinitySelector(t, nil, true, discovery.NodeTypeArchive, reversed)

	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 1000; i++ {
		req := hashRequest(randomHash(rng), true)
		na, err := a.GetNode(req, "")
		require.NoError(t, err)
		nb, err := b.GetNode(req, "")
		require.NoError(t, err)
		require.Equal(t, na.Key(), nb.Key())
	}
}

// A block number routes like a hash, but head-following requests stay random.
func TestBlockAffinityByNumber(t *testing.T) {
	r := newAffinitySelector(t, nil, true, discovery.NodeTypeArchive, affinityNodes(t, 22, discovery.NodeTypeArchive))
	number := rpc.BlockNumber(30_000_000)
	pinned := &types.RequestContext{
		ChainId:      affinityChain,
		BaseChainId:  affinityChain,
		Archive:      true,
		BlockContext: &types.BlockContext{Type: "Equals", BlockId: &rpc.BlockNumberOrHash{BlockNumber: &number}},
	}
	require.Equal(t, 1, distinctPicks(t, r, pinned))

	latest := rpc.LatestBlockNumber
	head := &types.RequestContext{
		ChainId:      affinityChain,
		BaseChainId:  affinityChain,
		Archive:      true,
		BlockContext: &types.BlockContext{Type: "Equals", BlockId: &rpc.BlockNumberOrHash{BlockNumber: &latest}},
	}
	require.Greater(t, distinctPicks(t, r, head), 1)

	hash := common.HexToHash("0x9da58660aa69f526fbc87f69448701d669c5228b94f618d4343672bef3e8b435")
	contains := hashRequest(hash, true)
	contains.BlockContext.Type = "Contains"
	require.Greater(t, distinctPicks(t, r, contains), 1)
}

// State nodes, unlisted chains, batches and native nodes keep the random pick.
func TestBlockAffinityOnlyArchive(t *testing.T) {
	hash := common.HexToHash("0xa2340245dad7d7616fd17c69ca5eda23fb91f7f428910290eb0ea1487c2c3175")

	off := newAffinitySelector(t, nil, false, discovery.NodeTypeArchive, affinityNodes(t, 22, discovery.NodeTypeArchive))
	require.Greater(t, distinctPicks(t, off, hashRequest(hash, true)), 1)

	// Only the listed chains: the same pool reached through another chain ID.
	other := New(utils.PickNodes, nil, map[string]struct{}{"1": {}})
	for _, node := range affinityNodes(t, 22, discovery.NodeTypeArchive) {
		require.NoError(t, other.UpsertNode(context.Background(), affinityChain, discovery.NodeTypeArchive, node))
	}
	require.Greater(t, distinctPicks(t, other, hashRequest(hash, true)), 1)

	state := newAffinitySelector(t, nil, true, discovery.NodeTypeState, affinityNodes(t, 22, discovery.NodeTypeState))
	require.Greater(t, distinctPicks(t, state, hashRequest(hash, false)), 1)

	on := newAffinitySelector(t, nil, true, discovery.NodeTypeArchive, affinityNodes(t, 22, discovery.NodeTypeArchive))
	require.Equal(t, 1, distinctPicks(t, on, hashRequest(hash, true)))

	batch := hashRequest(hash, true)
	batch.IsBatch = true
	require.Greater(t, distinctPicks(t, on, batch), 1)

	// Native nodes typed archive in etcd still get the random pick.
	native := New(utils.PickNodes, nil, affinityOn)
	for _, node := range affinityNodes(t, 22, discovery.NodeTypeArchive, lbnode.WithSource("native")) {
		require.NoError(t, native.UpsertNode(context.Background(), affinityChain, discovery.NodeTypeArchive, node))
	}
	seen := map[string]struct{}{}
	for i := 0; i < 200; i++ {
		node, err := native.GetNode(hashRequest(hash, true), "native")
		require.NoError(t, err)
		seen[node.Key()] = struct{}{}
	}
	require.Greater(t, len(seen), 1)
}

func TestBlockAffinityBalancedAndWeighted(t *testing.T) {
	nodes := affinityNodes(t, 22, discovery.NodeTypeArchive)
	gs := jsonrpc.NewGatewayStrategy()
	gs.UpdateGateway(affinityChain, discovery.Gateway{Weights: []discovery.WeightInfo{
		{NodeKey: nodes[0].Key(), Weight: 0},
		{NodeKey: nodes[1].Key(), Weight: 300},
	}})
	r := newAffinitySelector(t, gs, true, discovery.NodeTypeArchive, nodes)

	const rounds = 210_000
	counts := map[string]int{}
	rng := rand.New(rand.NewSource(2))
	for i := 0; i < rounds; i++ {
		node, err := r.GetNode(hashRequest(randomHash(rng), true), "")
		require.NoError(t, err)
		counts[node.Key()]++
	}

	// 20 nodes of weight 100 and one of 300: 2300 weight units in all.
	unit := float64(rounds) / 2300
	require.Zero(t, counts[nodes[0].Key()])
	require.InDelta(t, 300*unit, float64(counts[nodes[1].Key()]), 300*unit*0.05)
	for _, node := range nodes[2:] {
		require.InDelta(t, 100*unit, float64(counts[node.Key()]), 100*unit*0.05)
	}
}

// Removing a node moves only the blocks it owned.
func TestBlockAffinityRemoveMovesOnlyOwnedBlocks(t *testing.T) {
	nodes := affinityNodes(t, 22, discovery.NodeTypeArchive)
	r := newAffinitySelector(t, nil, true, discovery.NodeTypeArchive, nodes)

	rng := rand.New(rand.NewSource(3))
	hashes := make([]common.Hash, 20000)
	before := make([]string, len(hashes))
	for i := range hashes {
		hashes[i] = randomHash(rng)
		node, err := r.GetNode(hashRequest(hashes[i], true), "")
		require.NoError(t, err)
		before[i] = node.Key()
	}

	removed := nodes[5]
	require.NoError(t, r.RemoveNode(context.Background(), affinityChain, discovery.NodeTypeArchive, removed))
	for i, hash := range hashes {
		node, err := r.GetNode(hashRequest(hash, true), "")
		require.NoError(t, err)
		if before[i] != removed.Key() {
			require.Equal(t, before[i], node.Key())
		} else {
			require.NotEqual(t, removed.Key(), node.Key())
		}
	}
}

func BenchmarkGetNodeArchive(b *testing.B) {
	t := &testing.T{}
	nodes := affinityNodes(t, 22, discovery.NodeTypeArchive)
	hash := common.HexToHash("0x9da58660aa69f526fbc87f69448701d669c5228b94f618d4343672bef3e8b435")
	for _, affinity := range []bool{false, true} {
		r := newAffinitySelector(t, nil, affinity, discovery.NodeTypeArchive, nodes)
		req := hashRequest(hash, true)
		b.Run(fmt.Sprintf("affinity=%v", affinity), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if _, err := r.GetNode(req, ""); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
