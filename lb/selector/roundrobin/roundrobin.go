package roundrobin

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/Chaintable/nodex-proxy/discovery"
	"github.com/Chaintable/nodex-proxy/lb/lbnode"
	"github.com/Chaintable/nodex-proxy/lib/log"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/Chaintable/nodex-proxy/utils"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

type RoundRobin struct {
	archiveNodes map[string][]*lbnode.Node
	stateNodes   map[string][]*lbnode.Node
	nativeNodes  map[string][]*lbnode.Node
	chainHeight  map[string]*hexutil.Big
	pickNodeFunc utils.PickNodesFunc
	lock         sync.RWMutex
}

func New(pickNodeFunc utils.PickNodesFunc) *RoundRobin {
	return &RoundRobin{
		archiveNodes: make(map[string][]*lbnode.Node),
		stateNodes:   make(map[string][]*lbnode.Node),
		nativeNodes:  make(map[string][]*lbnode.Node),
		chainHeight:  make(map[string]*hexutil.Big),
		pickNodeFunc: pickNodeFunc,
	}
}

func (r *RoundRobin) GetNode(ctx *types.RequestContext, requestKey string) (*lbnode.Node, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	var best *lbnode.Node
	total := 0
	forceNative := requestKey == "native"
	nodes := r.pickNodeFunc(ctx.BlockContext, r.chainHeight[ctx.ChainId], r.archiveNodes[ctx.ChainId], r.stateNodes[ctx.ChainId], r.nativeNodes[ctx.ChainId], ctx.Archive, forceNative)

	// Log detailed information when no nodes are available after node selection
	if len(nodes) == 0 {
		archiveNodesCount := len(r.archiveNodes[ctx.ChainId])
		stateNodesCount := len(r.stateNodes[ctx.ChainId])
		var chainHeightStr string
		if r.chainHeight[ctx.ChainId] != nil {
			chainHeightStr = r.chainHeight[ctx.ChainId].String()
		} else {
			chainHeightStr = "nil"
		}

		log.Warn(fmt.Sprintf("No available nodes after selection for chain %s: archive_nodes_count=%d, state_nodes_count=%d, archive_mode=%t, chain_height=%s",
			ctx.ChainId, archiveNodesCount, stateNodesCount, ctx.Archive, chainHeightStr))
		return nil, utils.NewNoAvailableNodeError(ctx.ChainId, "no nodes available after selection")
	}

	for _, node := range nodes {
		node.IncrCurrentWeight(node.EffectWeight())
		total += node.EffectWeight()

		if best == nil || node.CurrentWeight() > best.CurrentWeight() {
			best = node
		}

	}

	if best == nil {
		log.Warn(fmt.Sprintf("Failed to select best node after weight calculation for chain %s: nodes_count=%d, total_weight=%d",
			ctx.ChainId, len(nodes), total))
		return nil, utils.NewNoAvailableNodeError(ctx.ChainId, "failed to select best node after weight calculation")
	}

	best.IncrCurrentWeight(total * -1)

	node := best.Clone()
	return node, nil
}

func (r *RoundRobin) UpsertNode(_ context.Context, chainId string, role discovery.NodeType, node *lbnode.Node) error {
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

func (r *RoundRobin) RemoveNode(_ context.Context, chainId string, role discovery.NodeType, node *lbnode.Node) error {
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

func (r *RoundRobin) UpdateChainHeight(_ context.Context, chainId string, chainHeight *hexutil.Big) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.chainHeight[chainId] = chainHeight
	return nil
}

func (r *RoundRobin) String() string {
	return "Weighted Round Robin"
}

func (r *RoundRobin) GetAllChainsIDs() []string {
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

func (r *RoundRobin) GetAllNodes(chainId string) ([]*lbnode.Node, bool) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	var nodes []*lbnode.Node
	nodes = append(nodes, r.archiveNodes[chainId]...)
	nodes = append(nodes, r.stateNodes[chainId]...)
	nodes = append(nodes, r.nativeNodes[chainId]...)
	return nodes, len(nodes) > 0
}

func (r *RoundRobin) GetArchiveNodes(chainId string) ([]*lbnode.Node, bool) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	nodes, exists := r.archiveNodes[chainId]
	return nodes, exists
}

func (r *RoundRobin) GetStateNodes(chainId string) ([]*lbnode.Node, bool) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	nodes, exists := r.stateNodes[chainId]
	return nodes, exists
}

func (r *RoundRobin) GetNativeNodes(chainId string) ([]*lbnode.Node, bool) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	nodes, exists := r.nativeNodes[chainId]
	return nodes, exists
}
