package roundrobin

import (
	"context"

	"github.com/Chaintable/nodex-proxy/lb/lbnode"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/Chaintable/nodex-proxy/utils"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

type RoundRobin struct {
	archiveNodes map[string][]*lbnode.Node
	stateNodes   map[string][]*lbnode.Node
	chainHeight  map[string]*hexutil.Big
	pickNodeFunc utils.PickNodesFunc
}

func New(pickNodeFunc utils.PickNodesFunc) *RoundRobin {
	return &RoundRobin{
		archiveNodes: make(map[string][]*lbnode.Node),
		stateNodes:   make(map[string][]*lbnode.Node),
		chainHeight:  make(map[string]*hexutil.Big),
		pickNodeFunc: pickNodeFunc,
	}
}

func (r *RoundRobin) GetNode(ctx *types.RequestContext, requestKey string) (*lbnode.Node, error) {
	var best *lbnode.Node
	total := 0
	nodes := r.pickNodeFunc(ctx.BlockContext, r.chainHeight[ctx.ChainId], r.archiveNodes[ctx.ChainId], r.stateNodes[ctx.ChainId])

	for _, node := range nodes {
		node.IncrCurrentWeight(node.EffectWeight())
		total += node.EffectWeight()

		if best == nil || node.CurrentWeight() > best.CurrentWeight() {
			best = node
		}

	}

	if best == nil {
		return nil, utils.ErrNoAvailableNode
	}

	best.IncrCurrentWeight(total * -1)

	node := best.Clone()
	return node, nil
}

func (r *RoundRobin) UpsertNode(_ context.Context, chainId string, role int, node *lbnode.Node) error {
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
func (r *RoundRobin) RemoveNode(_ context.Context, chainId string, role int, node *lbnode.Node) error {
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

func (r *RoundRobin) UpdateChainHeight(_ context.Context, chainId string, chainHeight *hexutil.Big) error {
	r.chainHeight[chainId] = chainHeight
	return nil
}

func (r *RoundRobin) String() string {
	return "Weighted Round Robin"
}
