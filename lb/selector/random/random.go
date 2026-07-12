package random

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/Chaintable/nodex-proxy/discovery"
	"github.com/Chaintable/nodex-proxy/lb/jsonrpc"
	"github.com/Chaintable/nodex-proxy/lb/lbnode"
	"github.com/Chaintable/nodex-proxy/lib/log"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/Chaintable/nodex-proxy/utils"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

type Random struct {
	archiveNodes    map[string][]*lbnode.Node
	stateNodes      map[string][]*lbnode.Node
	nativeNodes     map[string][]*lbnode.Node
	chainHeight     map[string]*hexutil.Big
	pickNodeFunc    utils.PickNodesFunc
	GatewayStrategy jsonrpc.GatewayStrategy
	lock            sync.RWMutex
}

func New(pickNodeFunc utils.PickNodesFunc, gatewayStrategy jsonrpc.GatewayStrategy) *Random {
	return &Random{
		archiveNodes:    make(map[string][]*lbnode.Node),
		stateNodes:      make(map[string][]*lbnode.Node),
		nativeNodes:     make(map[string][]*lbnode.Node),
		chainHeight:     make(map[string]*hexutil.Big),
		pickNodeFunc:    pickNodeFunc,
		GatewayStrategy: gatewayStrategy,
	}
}

func (r *Random) GetNode(ctx *types.RequestContext, requestKey string) (*lbnode.Node, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	chainId := ctx.ChainId
	archiveNodes := r.archiveNodes[chainId]
	stateNodes := r.stateNodes[chainId]
	nativeNodes := r.nativeNodes[chainId]
	forceNative := requestKey == "native"

	nodes := r.pickNodeFunc(ctx.BlockContext, r.chainHeight[chainId], archiveNodes, stateNodes, nativeNodes, ctx.Archive, forceNative)
	if len(nodes) == 0 {
		log.Warn(fmt.Sprintf("No nodes available after picking for chain %s: archive_nodes=%d, state_nodes=%d, archive_mode=%v, chain_height=%v",
			chainId, len(archiveNodes), len(stateNodes), ctx.Archive, r.chainHeight[chainId]))
		return nil, utils.NewNoAvailableNodeError(chainId, "no nodes available after picking")
	}

	// filter nodes by method route
	if r.GatewayStrategy != nil && !ctx.IsBatch {
		method := string(ctx.Method)
		originalCount := len(nodes)
		nodes = r.GatewayStrategy.FilterNodesByMethod(chainId, method, nodes)
		if len(nodes) == 0 {
			log.Warn(fmt.Sprintf("No nodes available after method filter for chain %s: method=%s, nodes_before=%d, nodes_after=0",
				chainId, method, originalCount))
			return nil, utils.NewNoAvailableNodeError(chainId, fmt.Sprintf("no nodes available after method filter for method %s", method))
		}
	}

	var weights map[string]int
	if r.GatewayStrategy != nil {
		weights, _ = r.GatewayStrategy.GetWeightForChain(chainId)
	}
	// Read-only lookup with a default: the weights map must never be written
	// here — request handlers run concurrently and a shared map would crash
	// the process on concurrent writes.
	nodeWeight := func(key string) int {
		if weight, exists := weights[key]; exists {
			return weight
		}
		return types.DefaultWeight
	}

	weightSum := 0
	for _, node := range nodes {
		weightSum += nodeWeight(node.Key())
	}

	if weightSum == 0 {
		log.Warn(fmt.Sprintf("Weight sum is zero for chain %s: nodes_count=%d, weights_count=%d",
			chainId, len(nodes), len(weights)))
		return nil, utils.NewNoAvailableNodeError(chainId, "weight sum is zero, all nodes have zero weight")
	}

	targetWeight := utils.RangeRandom(0, int64(weightSum))
	curWeight := 0
	for _, node := range nodes {
		curWeight += nodeWeight(node.Key())
		if int64(curWeight) >= targetWeight {
			finalNode := node.Clone()
			return finalNode, nil
		}
	}

	// This should never happen, but handle it gracefully
	log.Warn(fmt.Sprintf("Failed to select node after weight calculation for chain %s: nodes_count=%d, weight_sum=%d, target_weight=%d",
		chainId, len(nodes), weightSum, targetWeight))
	return nil, utils.NewNoAvailableNodeError(chainId, "failed to select node after weight calculation")
}

func (r *Random) UpsertNode(_ context.Context, chainId string, role discovery.NodeType, node *lbnode.Node) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if node.Source() == "native" {
		r.nativeNodes[chainId] = lbnode.UpsertInto(r.nativeNodes[chainId], node)
		return nil
	}

	if role == discovery.NodeTypeArchive {
		r.archiveNodes[chainId] = lbnode.UpsertInto(r.archiveNodes[chainId], node)
	} else {
		r.stateNodes[chainId] = lbnode.UpsertInto(r.stateNodes[chainId], node)
	}
	return nil
}

func (r *Random) RemoveNode(_ context.Context, chainId string, role discovery.NodeType, node *lbnode.Node) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if node.Source() == "native" {
		nodes, exists := r.nativeNodes[chainId]
		if !exists {
			return nil
		}
		for i, existingNode := range nodes {
			if existingNode.Key() == node.Key() {
				r.nativeNodes[chainId] = append(nodes[:i], nodes[i+1:]...)
				return nil
			}
		}
		return nil
	}

	if role == discovery.NodeTypeArchive {
		nodes, exists := r.archiveNodes[chainId]
		if !exists {
			return nil
		}

		for i, existingNode := range nodes {
			if existingNode.Key() == node.Key() {
				r.archiveNodes[chainId] = append(nodes[:i], nodes[i+1:]...)
				return nil
			}
		}
	} else {
		nodes, exists := r.stateNodes[chainId]
		if !exists {
			return nil
		}

		for i, existingNode := range nodes {
			if existingNode.Key() == node.Key() {
				r.stateNodes[chainId] = append(nodes[:i], nodes[i+1:]...)
				return nil
			}
		}
	}
	return nil
}

func (r *Random) UpdateChainHeight(_ context.Context, chainId string, chainHeight *hexutil.Big) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.chainHeight[chainId] = chainHeight
	return nil
}

func (r *Random) String() string {
	return "Random"
}

func (r *Random) GetAllNodes(chainId string) ([]*lbnode.Node, bool) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	var nodes []*lbnode.Node
	nodes = append(nodes, r.archiveNodes[chainId]...)
	nodes = append(nodes, r.stateNodes[chainId]...)
	nodes = append(nodes, r.nativeNodes[chainId]...)
	return nodes, len(nodes) > 0
}

func (r *Random) GetArchiveNodes(chainId string) ([]*lbnode.Node, bool) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	nodes, exists := r.archiveNodes[chainId]
	return nodes, exists
}

func (r *Random) GetStateNodes(chainId string) ([]*lbnode.Node, bool) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	nodes, exists := r.stateNodes[chainId]
	return nodes, exists
}

func (r *Random) GetNativeNodes(chainId string) ([]*lbnode.Node, bool) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	nodes, exists := r.nativeNodes[chainId]
	return nodes, exists
}

func (r *Random) GetAllChainsIDs() []string {
	r.lock.RLock()
	defer r.lock.RUnlock()

	chainsMap := make(map[string]bool)
	for chainId := range r.archiveNodes {
		chainsMap[chainId] = true
	}
	for chainId := range r.stateNodes {
		chainsMap[chainId] = true
	}
	for chainId := range r.nativeNodes {
		chainsMap[chainId] = true
	}

	chains := make([]string, 0, len(chainsMap))
	for chainId := range chainsMap {
		chains = append(chains, chainId)
	}
	sort.Strings(chains)
	return chains
}
