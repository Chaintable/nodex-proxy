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
}

type ChainHeight struct {
	ChainId           string       `json:"-"`
	LatestBlockNumber *hexutil.Big `json:"latestBlockNumber"`
}

type Discover interface {
	Init(ctx context.Context) (<-chan *TargetNode, <-chan *ChainHeight, error)
	Close() error
}
