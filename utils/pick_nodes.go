package utils

import (
	"math/big"

	"github.com/Chaintable/nodex-proxy/lb/lbnode"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
)

// PickNodesFunc receives the block context as a getter so implementations only
// pay for parsing it when the state/archive decision actually depends on it.
type PickNodesFunc func(getBlockContext func() *types.BlockContext, blockHeight *hexutil.Big, archiveNodes []*lbnode.Node, stateNodes []*lbnode.Node, nativeNodes []*lbnode.Node, forceArchive bool, forceNative bool) []*lbnode.Node

// preferAvailableNodes returns available nodes if any exist, otherwise returns
// all nodes as fallback. When every node is available it returns the input
// slice itself, so callers of PickNodes may receive a slice sharing the
// selector pool's backing array — treat the result as read-only.
func preferAvailableNodes(nodes []*lbnode.Node) []*lbnode.Node {
	if len(nodes) == 0 {
		return nodes
	}

	allAvailable := true
	for _, node := range nodes {
		if !node.Available() {
			allAvailable = false
			break
		}
	}
	if allAvailable {
		return nodes
	}

	available := make([]*lbnode.Node, 0, len(nodes))
	for _, node := range nodes {
		if node.Available() {
			available = append(available, node)
		}
	}

	if len(available) > 0 {
		return available
	}
	return nodes
}

func PickNodes(getBlockContext func() *types.BlockContext, blockHeight *hexutil.Big, archiveNodes []*lbnode.Node, stateNodes []*lbnode.Node, nativeNodes []*lbnode.Node, forceArchive bool, forceNative bool) []*lbnode.Node {
	// Prefer available nodes for each node type
	archiveNodes = preferAvailableNodes(archiveNodes)
	stateNodes = preferAvailableNodes(stateNodes)
	nativeNodes = preferAvailableNodes(nativeNodes)

	if forceNative {
		return nativeNodes
	}
	if forceArchive {
		return archiveNodes
	}

	if len(stateNodes) == 0 {
		stateNodes = archiveNodes
	}
	if len(archiveNodes) == 0 {
		archiveNodes = stateNodes
	}

	// When both pools resolve to the same node set (single-pool chain), the
	// block context cannot change the outcome — skip parsing it entirely.
	if len(stateNodes) == len(archiveNodes) &&
		(len(stateNodes) == 0 || &stateNodes[0] == &archiveNodes[0]) {
		return stateNodes
	}

	var blockContext *types.BlockContext
	if getBlockContext != nil {
		blockContext = getBlockContext()
	}
	// Only a request pinned to a block number can need archive; Contains and
	// hash lookups stay on state (the -39006 retry covers misses).
	if blockContext == nil || blockContext.Type != "Equals" ||
		blockContext.BlockId == nil || blockContext.BlockId.BlockNumber == nil {
		return stateNodes
	}
	blockNumber := *blockContext.BlockId.BlockNumber
	if blockNumber == rpc.LatestBlockNumber || blockNumber == rpc.PendingBlockNumber {
		return stateNodes
	}
	if blockHeight == nil {
		// Unknown chain height: only archive nodes are guaranteed to
		// hold the requested historical block.
		return archiveNodes
	}
	// State nodes keep the latest 64 blocks.
	stateLow := new(big.Int).Sub(blockHeight.ToInt(), big.NewInt(64))
	if big.NewInt(blockNumber.Int64()).Cmp(stateLow) >= 0 {
		return stateNodes
	}
	return archiveNodes
}
