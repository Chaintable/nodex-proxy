package random

import (
	"context"
	"sync"

	"github.com/Chaintable/nodex-proxy/lb/lbnode"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/Chaintable/nodex-proxy/utils"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

type Random struct {
	archiveNodes map[string][]*lbnode.Node
	stateNodes   map[string][]*lbnode.Node
	chainHeight  map[string]*hexutil.Big
	pickNodeFunc utils.PickNodesFunc
	lock         sync.RWMutex
}

func New(pickNodeFunc utils.PickNodesFunc) *Random {
	return &Random{
		archiveNodes: make(map[string][]*lbnode.Node),
		stateNodes:   make(map[string][]*lbnode.Node),
		chainHeight:  make(map[string]*hexutil.Big),
		pickNodeFunc: pickNodeFunc,
	}
}

func (r *Random) GetNode(ctx *types.RequestContext, _ string) (*lbnode.Node, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	var tempNodes []*lbnode.Node
	nodes := r.pickNodeFunc(ctx.BlockContext, r.chainHeight[ctx.ChainId], r.archiveNodes[ctx.ChainId], r.stateNodes[ctx.ChainId])

	weightSum := 0
	for _, node := range nodes {
		tempNodes = append(tempNodes, node)
		weightSum += node.EffectWeight()
	}

	if len(tempNodes) == 0 {
		return nil, utils.ErrNoAvailableNode
	}
	returnWeight := utils.RangeRandom(0, int64(weightSum))
	tempWeight := 0
	for _, node := range tempNodes {
		tempWeight += node.EffectWeight()
		if int64(tempWeight) >= returnWeight {
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
