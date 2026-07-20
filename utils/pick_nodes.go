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
type PickNodesFunc func(getBlockContext func() *types.BlockContext, blockHeight *hexutil.Big, archiveNodes []*lbnode.Node, stateNodes []*lbnode.Node, nativeNodes []*lbnode.Node, fourceArchive bool, fourceNative bool) []*lbnode.Node

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

func PickNodes(getBlockContext func() *types.BlockContext, blockHeight *hexutil.Big, archiveNodes []*lbnode.Node, stateNodes []*lbnode.Node, nativeNodes []*lbnode.Node, fourceArchive bool, fourceNative bool) []*lbnode.Node {
	// Prefer available nodes for each node type
	archiveNodes = preferAvailableNodes(archiveNodes)
	stateNodes = preferAvailableNodes(stateNodes)
	nativeNodes = preferAvailableNodes(nativeNodes)

	if fourceNative {
		return nativeNodes
	}
	if fourceArchive {
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

	// stateNodes strategy:
	// when Equals and blockNumber is LatestBlockNumber or PendingBlockNumber, return stateNodes
	// when Equals and blockNumber is less than 64 blocks behind the latest block, return stateNodes
	// when Equals and blockNumber is more than 64 blocks behind the latest block, return archiveNodes
	// when Contains, return stateNodes
	if blockContext != nil {
		if blockContext.Type == "Equals" && blockContext.BlockId != nil && blockContext.BlockId.BlockNumber != nil {
			if *blockContext.BlockId.BlockNumber == rpc.LatestBlockNumber ||
				*blockContext.BlockId.BlockNumber == rpc.PendingBlockNumber {
				return stateNodes
			}
			if blockHeight == nil {
				// Unknown chain height: only archive nodes are guaranteed to
				// hold the requested historical block.
				return archiveNodes
			}
			stateBlockHeightLow := big.NewInt(0)
			stateBlockHeightLow.Sub(blockHeight.ToInt(), big.NewInt(64))
			if big.NewInt(blockContext.BlockId.BlockNumber.Int64()).Cmp(stateBlockHeightLow) >= 0 {
				return stateNodes
			} else {
				return archiveNodes
			}
		} else if blockContext.Type == "Contains" {
			return stateNodes
		} else {
			return stateNodes
		}
	}
	return stateNodes
}
