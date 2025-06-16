package jsonrpc

import (
	"sync"

	"github.com/Chaintable/nodex-proxy/discovery"
)

type GatewayStrategy interface {
	GetWeightForChain(chainId string) (map[string]int, bool)
	GetWeightForNode(chainId string, nodeId string) (int, bool)
	UpdateWeightForChain(chainId string, status []discovery.GatewayStatus)
	DeleteWeightForChain(chainId string)
}

type gatewayStrategy struct {
	// chainid -> nodeid -> weight
	gatewayMap map[string]map[string]int
	sync.RWMutex
}

func NewGatewayStrategy() GatewayStrategy {
	return &gatewayStrategy{
		gatewayMap: make(map[string]map[string]int),
	}
}

func (g *gatewayStrategy) GetWeightForChain(chainId string) (map[string]int, bool) {
	g.RLock()
	defer g.RUnlock()

	nodes, exists := g.gatewayMap[chainId]
	if !exists {
		// return empty map if not exists
		nodes = make(map[string]int)
	}
	return nodes, exists
}

func (g *gatewayStrategy) GetWeightForNode(chainId string, nodeId string) (int, bool) {
	g.RLock()
	defer g.RUnlock()

	if nodes, exists := g.gatewayMap[chainId]; exists {
		if weight, exists := nodes[nodeId]; exists {
			return weight, true
		}
	}
	return 0, false
}

func (g *gatewayStrategy) UpdateWeightForChain(chainId string, status []discovery.GatewayStatus) {
	g.Lock()
	defer g.Unlock()

	g.gatewayMap[chainId] = make(map[string]int)
	for _, status := range status {
		g.gatewayMap[chainId][status.NodeKey] = status.Weight
	}
}

func (g *gatewayStrategy) DeleteWeightForChain(chainId string) {
	g.Lock()
	defer g.Unlock()

	delete(g.gatewayMap, chainId)
}
