package discovery

import (
	"context"

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
