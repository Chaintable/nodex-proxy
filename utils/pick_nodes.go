package utils

import (
	"math/big"

	"github.com/Chaintable/nodex-proxy/lb/lbnode"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

type PickNodesFunc func(blockContext *types.BlockContext, blockHeight *hexutil.Big, archiveNodes []*lbnode.Node, stateNodes []*lbnode.Node) []*lbnode.Node

func PickNodes(blockContext *types.BlockContext, blockHeight *hexutil.Big, archiveNodes []*lbnode.Node, stateNodes []*lbnode.Node) []*lbnode.Node {
	var backupNodes []*lbnode.Node
	backupNodes = append(backupNodes, stateNodes...)
	backupNodes = append(backupNodes, archiveNodes...)

	if len(stateNodes) == 0 {
		stateNodes = archiveNodes
	}
	if len(archiveNodes) == 0 {
		archiveNodes = stateNodes
	}
	if blockContext != nil {
		if blockContext.Type == "equal" && blockContext.BlockId.BlockNumber != nil {
			stateBlockHeightLow := big.NewInt(0)
			stateBlockHeightLow.Sub(blockHeight.ToInt(), big.NewInt(64))
			if big.NewInt(blockContext.BlockId.BlockNumber.Int64()).Cmp(stateBlockHeightLow) >= 0 {
				return stateNodes
			} else {
				return archiveNodes
			}
		} else {
			return archiveNodes
		}
	}
	return backupNodes
}
