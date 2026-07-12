package jsonrpc

import (
	"sync"

	"github.com/Chaintable/nodex-proxy/discovery"
	"github.com/Chaintable/nodex-proxy/lb/lbnode"
)

type GatewayStrategy interface {
	// weight management
	GetWeightForChain(chainId string) (map[string]int, bool)
	GetWeightForNode(chainId string, nodeId string) (int, bool)
	UpdateWeightForChain(chainId string, status []discovery.WeightInfo)

	// method route management
	GetMethodRoutes(chainId string) ([]discovery.MethodRoute, bool)
	AddMethodRoute(chainId string, method string, route discovery.MethodRoute) error
	FilterNodesByMethod(chainId, method string, nodes []*lbnode.Node) []*lbnode.Node

	// gateway management
	UpdateGateway(chainId string, gateway discovery.Gateway)
	DeleteGateway(chainId string)
}

type gatewayStrategy struct {
	// chainid -> nodeid -> weight
	weightMap map[string]map[string]int
	// chainid -> method -> route
	methodRouteMap map[string]map[string]discovery.MethodRoute
	sync.RWMutex
}

func NewGatewayStrategy() GatewayStrategy {
	return &gatewayStrategy{
		weightMap:      make(map[string]map[string]int),
		methodRouteMap: make(map[string]map[string]discovery.MethodRoute),
	}
}

func (g *gatewayStrategy) GetWeightForChain(chainId string) (map[string]int, bool) {
	g.RLock()
	defer g.RUnlock()

	nodes, exists := g.weightMap[chainId]
	// Always return a copy: callers (e.g. selectors on the request hot path)
	// must never share the internal map, or concurrent reads/writes crash the
	// process with a fatal concurrent map error.
	copied := make(map[string]int, len(nodes))
	for k, v := range nodes {
		copied[k] = v
	}
	return copied, exists
}

func (g *gatewayStrategy) GetWeightForNode(chainId string, nodeId string) (int, bool) {
	g.RLock()
	defer g.RUnlock()

	if nodes, exists := g.weightMap[chainId]; exists {
		if weight, exists := nodes[nodeId]; exists {
			return weight, true
		}
	}
	return 0, false
}

func (g *gatewayStrategy) UpdateWeightForChain(chainId string, status []discovery.WeightInfo) {
	g.Lock()
	defer g.Unlock()

	g.weightMap[chainId] = make(map[string]int)
	for _, status := range status {
		g.weightMap[chainId][status.NodeKey] = status.Weight
	}
}

func (g *gatewayStrategy) UpdateGateway(chainId string, gateway discovery.Gateway) {
	g.Lock()
	defer g.Unlock()

	// 更新权重
	g.weightMap[chainId] = make(map[string]int)
	for _, weightInfo := range gateway.Weights {
		g.weightMap[chainId][weightInfo.NodeKey] = weightInfo.Weight
	}

	// 更新方法路由
	g.methodRouteMap[chainId] = make(map[string]discovery.MethodRoute)
	for method, route := range gateway.MethodRoutes {
		g.methodRouteMap[chainId][method] = route
	}
}

func (g *gatewayStrategy) GetMethodRoutes(chainId string) ([]discovery.MethodRoute, bool) {
	g.RLock()
	defer g.RUnlock()

	var allRoutes []discovery.MethodRoute
	if methodRoutes, exists := g.methodRouteMap[chainId]; exists {
		for _, route := range methodRoutes {
			allRoutes = append(allRoutes, route)
		}
		return allRoutes, true
	}
	return nil, false
}

func (g *gatewayStrategy) AddMethodRoute(chainId string, method string, route discovery.MethodRoute) error {
	g.Lock()
	defer g.Unlock()

	if g.methodRouteMap[chainId] == nil {
		g.methodRouteMap[chainId] = make(map[string]discovery.MethodRoute)
	}

	// if method route exists, replace it
	g.methodRouteMap[chainId][method] = route
	return nil
}

func (g *gatewayStrategy) DeleteGateway(chainId string) {
	g.Lock()
	defer g.Unlock()

	delete(g.weightMap, chainId)
	delete(g.methodRouteMap, chainId)
}

func (g *gatewayStrategy) FilterNodesByMethod(chainId, method string, nodes []*lbnode.Node) []*lbnode.Node {
	g.RLock()
	defer g.RUnlock()

	if methodRoutes, exists := g.methodRouteMap[chainId]; exists {
		if routes, methodExists := methodRoutes[method]; methodExists {
			return g.applyMethodRoutes(routes, nodes)
		}
	}
	return nodes // if no method route, return all nodes
}

func (g *gatewayStrategy) applyMethodRoutes(routes discovery.MethodRoute, nodes []*lbnode.Node) []*lbnode.Node {
	// rule:
	// - if include nodes is not empty, only include nodes will be returned
	// - else if exclude nodes is not empty, all nodes will be returned except exclude nodes
	// - else all nodes will be returned

	// include nodes
	if len(routes.IncludeNodeKeys) > 0 {
		var filteredNodes []*lbnode.Node
		for _, node := range nodes {
			if routes.IncludeNodeKeys[node.Key()] {
				filteredNodes = append(filteredNodes, node)
			}
		}
		return filteredNodes
	}

	// exclude nodes
	if len(routes.ExcludeNodeKeys) > 0 {
		var filteredNodes []*lbnode.Node

		for _, node := range nodes {
			if !routes.ExcludeNodeKeys[node.Key()] {
				filteredNodes = append(filteredNodes, node)
			}
		}
		return filteredNodes
	}

	return nodes
}
