package discovery

import (
	"context"

	"github.com/Chaintable/nodex-proxy/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

type TargetNode struct {
	ChainId    string `json:"-"`
	StateType  int    `json:"stateType"` // 1 latest, 2 delay, 3 offline
	Address    string `json:"address"`   //
	Port       int    `json:"port"`
	NodeType   int    `json:"nodeType"` // 1 state, 2 archive
	ChangeType int    `json:"-"`
	NodeKey    string `json:"-"`
	Weight     int    `json:"weight"` // 0-100
	Source     string `json:"source"` // manual, official
}

type ChainHeight struct {
	ChainId           string       `json:"-"`
	LatestBlockNumber *hexutil.Big `json:"latestBlockNumber"`
}

type Gateway struct {
	ChainId    string       `json:"-"`
	ChangeType int          `json:"-"`
	Weights    []WeightInfo `json:"weights,omitempty"`
	// method -> methodRoute
	MethodRoutes map[string]MethodRoute `json:"method_routes,omitempty"`
}

func (g *Gateway) SetWeight(nodeKey string, weight int) {
	nodeIndex := -1
	for i, weightInfo := range g.Weights {
		if weightInfo.NodeKey == nodeKey {
			nodeIndex = i
			break
		}
	}

	// only add to gateway if weight is not default(100)
	if weight == types.DefaultWeight {
		if nodeIndex != -1 {
			// if node exists, remove it from gateway
			g.Weights = append(g.Weights[:nodeIndex], g.Weights[nodeIndex+1:]...)
		}
		// if node doesn't exist, do nothing
	} else {
		if nodeIndex != -1 {
			// if node exists, update its weight
			g.Weights[nodeIndex].Weight = weight
		} else {
			// if node doesn't exist, add it to gateway
			g.Weights = append(g.Weights, WeightInfo{
				NodeKey: nodeKey,
				Weight:  weight,
			})
		}
	}
}

func (g *Gateway) ClearMethodRoute(nodeKey string) {
	if g.MethodRoutes != nil {
		for method, route := range g.MethodRoutes {
			// Remove node from include_node_keys
			delete(route.IncludeNodeKeys, nodeKey)
			// Remove node from exclude_node_keys
			delete(route.ExcludeNodeKeys, nodeKey)

			// If both include and exclude are empty, remove the entire method route
			if len(route.IncludeNodeKeys) == 0 && len(route.ExcludeNodeKeys) == 0 {
				delete(g.MethodRoutes, method)
			} else {
				g.MethodRoutes[method] = route
			}
		}
	}
}

type WeightInfo struct {
	NodeKey string `json:"node_key"`
	Weight  int    `json:"weight"`
}

type MethodRoute struct {
	IncludeNodeKeys map[string]bool `json:"include_node_keys"`
	ExcludeNodeKeys map[string]bool `json:"exclude_node_keys"`
}

type Discover interface {
	Init(ctx context.Context) (<-chan *TargetNode, <-chan *ChainHeight, <-chan *Gateway, error)
	Close() error
}
