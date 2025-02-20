package roundrobin

import (
	"context"
	"github.com/Chaintable/nodex-proxy/lb/lbnode"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/Chaintable/nodex-proxy/utils"
	"github.com/ethereum/go-ethereum/common/hexutil"
	cmap "github.com/orcaman/concurrent-map/v2"
)

type RoundRobin struct {
	archiveNodes cmap.ConcurrentMap[string, []*lbnode.Node] //map[string][]*lbnode.Node
	stateNodes   cmap.ConcurrentMap[string, []*lbnode.Node] //map[string][]*lbnode.Node
	chainHeight  cmap.ConcurrentMap[string, *hexutil.Big]   //map[string]*hexutil.Big
	pickNodeFunc utils.PickNodesFunc
}

func New(pickNodeFunc utils.PickNodesFunc) *RoundRobin {
	return &RoundRobin{
		archiveNodes: cmap.New[[]*lbnode.Node](),
		stateNodes:   cmap.New[[]*lbnode.Node](),
		chainHeight:  cmap.New[*hexutil.Big](),
		pickNodeFunc: pickNodeFunc,
	}
}

func (r *RoundRobin) GetNode(ctx *types.RequestContext, requestKey string) (*lbnode.Node, error) {
	var best *lbnode.Node
	total := 0
	height, _ := r.chainHeight.Get(ctx.ChainId)
	archiveNodes, _ := r.archiveNodes.Get(ctx.ChainId)
	stateNodes, _ := r.stateNodes.Get(ctx.ChainId)
	nodes := r.pickNodeFunc(ctx.BlockContext, height, archiveNodes, stateNodes)

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
		nodes, exists := r.archiveNodes.Get(chainId)
		if !exists {
			r.archiveNodes.Set(chainId, []*lbnode.Node{node})
			return nil
		}

		for i, existingNode := range nodes {
			if existingNode.Key() == node.Key() {
				// 更新现有节点
				nodes[i] = node
				return nil
			}
		}

		r.archiveNodes.Set(chainId, append(nodes, node))
	} else {
		nodes, exists := r.stateNodes.Get(chainId)
		if !exists {
			r.stateNodes.Set(chainId, []*lbnode.Node{node})
			return nil
		}
		for i, existingNode := range nodes {
			if existingNode.Key() == node.Key() {
				nodes[i] = node
				return nil
			}
		}
		r.stateNodes.Set(chainId, append(nodes, node))
	}
	return nil
}
func (r *RoundRobin) RemoveNode(_ context.Context, chainId string, role int, node *lbnode.Node) error {
	if role == 2 {
		nodes, exists := r.archiveNodes.Get(chainId)
		if !exists {
			return nil
		}

		for i, existingNode := range nodes {
			if existingNode.Key() == node.Key() {
				r.archiveNodes.Set(chainId, append(nodes[:i], nodes[i+1:]...))
				return nil
			}
		}
	} else {
		nodes, exists := r.stateNodes.Get(chainId)
		if !exists {
			return nil
		}

		for i, existingNode := range nodes {
			if existingNode.Key() == node.Key() {
				r.stateNodes.Set(chainId, append(nodes[:i], nodes[i+1:]...))
				return nil
			}
		}
	}
	return nil
}

func (r *RoundRobin) UpdateChainHeight(_ context.Context, chainId string, chainHeight *hexutil.Big) error {
	r.chainHeight.Set(chainId, chainHeight)
	return nil
}

func (r *RoundRobin) String() string {
	return "Weighted Round Robin"
}
