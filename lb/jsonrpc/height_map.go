package jsonrpc

import (
	"sync"

	"github.com/ethereum/go-ethereum/common/hexutil"
)

type HeightMap interface {
	SetHeight(chainId string, height *hexutil.Big)
	GetHeight(chainId string) *hexutil.Big
}

type heightMap struct {
	heightMap map[string]*hexutil.Big
	sync.RWMutex
}

func NewHeightMap() HeightMap {
	return &heightMap{
		heightMap: make(map[string]*hexutil.Big),
	}
}

func (hm *heightMap) SetHeight(chainId string, height *hexutil.Big) {
	hm.Lock()
	defer hm.Unlock()
	hm.heightMap[chainId] = height
}

func (hm *heightMap) GetHeight(chainId string) *hexutil.Big {
	hm.RLock()
	defer hm.RUnlock()
	return hm.heightMap[chainId]
}
