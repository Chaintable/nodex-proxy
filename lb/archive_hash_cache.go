package lb

import (
	"container/list"
	"sync"

	"github.com/Chaintable/nodex-proxy/types"
	"github.com/ethereum/go-ethereum/common"
)

// archiveHashCache remembers, per chain, block hashes that state nodes could
// not serve (-39006) but archive nodes could. A state node only keeps a short
// window of recent blocks in memory and cannot tell how old a hash is, so a
// hash-addressed historical request normally costs a wasted state hop before
// the archive retry. Hits on this cache go straight to archive instead.
//
// A nil *archiveHashCache is a valid, disabled cache.
type archiveHashCache struct {
	size   int
	chains sync.Map // chainId -> *hashLRU
}

func newArchiveHashCache(size int) *archiveHashCache {
	if size <= 0 {
		return nil
	}
	return &archiveHashCache{size: size}
}

// Contains reports whether hash is known to need an archive node on chainId,
// and marks it as recently used.
func (c *archiveHashCache) Contains(chainId string, hash common.Hash) bool {
	l := c.chain(chainId)
	if l == nil {
		return false
	}
	return l.Get(hash)
}

// HasChain reports whether chainId has any cached hash, so callers can skip
// parsing the request's block context on chains that never needed it.
func (c *archiveHashCache) HasChain(chainId string) bool {
	return c.chain(chainId) != nil
}

func (c *archiveHashCache) Add(chainId string, hash common.Hash) {
	if c == nil {
		return
	}
	v, ok := c.chains.Load(chainId)
	if !ok {
		v, _ = c.chains.LoadOrStore(chainId, newHashLRU(c.size))
	}
	v.(*hashLRU).Add(hash)
}

func (c *archiveHashCache) chain(chainId string) *hashLRU {
	if c == nil {
		return nil
	}
	v, ok := c.chains.Load(chainId)
	if !ok {
		return nil
	}
	return v.(*hashLRU)
}

// hashLRU is a fixed-capacity LRU set of block hashes.
type hashLRU struct {
	mu    sync.Mutex
	size  int
	order *list.List // front = most recently used; values are common.Hash
	items map[common.Hash]*list.Element
}

func newHashLRU(size int) *hashLRU {
	return &hashLRU{
		size:  size,
		order: list.New(),
		items: make(map[common.Hash]*list.Element, size),
	}
}

func (l *hashLRU) Get(hash common.Hash) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.items[hash]
	if ok {
		l.order.MoveToFront(e)
	}
	return ok
}

func (l *hashLRU) Add(hash common.Hash) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e, ok := l.items[hash]; ok {
		l.order.MoveToFront(e)
		return
	}
	if l.order.Len() >= l.size {
		oldest := l.order.Back()
		l.order.Remove(oldest)
		delete(l.items, oldest.Value.(common.Hash))
	}
	l.items[hash] = l.order.PushFront(hash)
}

func (l *hashLRU) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.order.Len()
}

// requestBlockHash returns the block hash the request is pinned to, if any.
func requestBlockHash(requestContext *types.RequestContext) (common.Hash, bool) {
	blockContext := requestContext.GetBlockContext()
	if blockContext == nil || blockContext.BlockId == nil || blockContext.BlockId.BlockHash == nil {
		return common.Hash{}, false
	}
	return *blockContext.BlockId.BlockHash, true
}
