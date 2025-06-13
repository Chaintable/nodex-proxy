package random

import (
	"context"
	"sort"
	"sync"

	"github.com/Chaintable/nodex-proxy/lb/jsonrpc"
	"github.com/Chaintable/nodex-proxy/lb/lbnode"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/Chaintable/nodex-proxy/utils"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

type Random struct {
	archiveNodes    map[string][]*lbnode.Node
	stateNodes      map[string][]*lbnode.Node
	chainHeight     map[string]*hexutil.Big
	pickNodeFunc    utils.PickNodesFunc
	GatewayStrategy jsonrpc.GatewayStrategy
	lock            sync.RWMutex
}

func New(pickNodeFunc utils.PickNodesFunc, gatewayStrategy jsonrpc.GatewayStrategy) *Random {
	return &Random{
		archiveNodes:    make(map[string][]*lbnode.Node),
		stateNodes:      make(map[string][]*lbnode.Node),
		chainHeight:     make(map[string]*hexutil.Big),
		pickNodeFunc:    pickNodeFunc,
		GatewayStrategy: gatewayStrategy,
	}
}

func (r *Random) GetNode(ctx *types.RequestContext, _ string) (*lbnode.Node, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	nodes := r.pickNodeFunc(ctx.BlockContext, r.chainHeight[ctx.ChainId], r.archiveNodes[ctx.ChainId], r.stateNodes[ctx.ChainId])
	if len(nodes) == 0 {
		return nil, utils.ErrNoAvailableNode
	}

	weights, _ := r.GatewayStrategy.GetWeightForChain(ctx.ChainId)

	weightSum := 0
	for _, node := range nodes {
		weight, exists := weights[node.Key()]
		if !exists {
			// if not exists in gateway, set default weight
			weights[node.Key()] = types.DefaultWeight
			weightSum += types.DefaultWeight
		} else {
			weightSum += weight
		}
	}

	targetWeight := utils.RangeRandom(0, int64(weightSum))
	curWeight := 0
	for _, node := range nodes {
		curWeight += weights[node.Key()]
		if int64(curWeight) >= targetWeight {
			finalNode := node.Clone()
			return finalNode, nil
		}
	}

	return nil, utils.ErrNoAvailableNode
}

func (r *Random) UpsertNode(_ context.Context, chainId string, role int, node *lbnode.Node) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if role == 2 {
		nodes, exists := r.archiveNodes[chainId]
		if !exists {
			r.archiveNodes[chainId] = []*lbnode.Node{node}
			return nil
		}

		for i, existingNode := range nodes {
			if existingNode.Key() == node.Key() {
				// 更新现有节点
				nodes[i] = node
				return nil
			}
		}

		r.archiveNodes[chainId] = append(nodes, node)
	} else {
		nodes, exists := r.stateNodes[chainId]
		if !exists {
			r.stateNodes[chainId] = []*lbnode.Node{node}
			return nil
		}
		for i, existingNode := range nodes {
			if existingNode.Key() == node.Key() {
				nodes[i] = node
				return nil
			}
		}
		r.stateNodes[chainId] = append(nodes, node)
	}
	return nil
}

func (r *Random) RemoveNode(_ context.Context, chainId string, role int, node *lbnode.Node) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if role == 2 {
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

	chains := make([]string, 0, len(chainsMap))
	for chainId := range chainsMap {
		chains = append(chains, chainId)
	}
	sort.Strings(chains)
	return chains
}
