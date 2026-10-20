package lb

import (
	"sync"

	"github.com/Chaintable/nodex-proxy/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/maypok86/otter/v2"
)

// archiveHashCache remembers, per chain, block hashes that state nodes could
// not serve (-39006) but archive nodes could. A state node only keeps a short
// window of recent blocks in memory and cannot tell how old a hash is, so a
// hash-addressed historical request normally costs a wasted state hop before
// the archive retry. Hits on this cache go straight to archive instead.
//
// Each chain gets its own bounded otter cache (adaptive W-TinyLFU, lock-free
// reads), so one chain's traffic cannot evict another's hashes. A miss only
// costs the old state-then-archive path.
//
// A nil *archiveHashCache is a valid, disabled cache.
type archiveHashCache struct {
	size   int
	chains sync.Map // chainId -> *otter.Cache[common.Hash, struct{}]
}

func newArchiveHashCache(size int) *archiveHashCache {
	if size <= 0 {
		return nil
	}
	return &archiveHashCache{size: size}
}

// Contains reports whether hash is known to need an archive node on chainId.
func (c *archiveHashCache) Contains(chainId string, hash common.Hash) bool {
	cache := c.chain(chainId)
	if cache == nil {
		return false
	}
	_, ok := cache.GetIfPresent(hash)
	return ok
}

// HasChain reports whether chainId has ever cached a hash, so callers can skip
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
		v, _ = c.chains.LoadOrStore(chainId, otter.Must(&otter.Options[common.Hash, struct{}]{
			MaximumSize: c.size,
		}))
	}
	v.(*otter.Cache[common.Hash, struct{}]).Set(hash, struct{}{})
}

func (c *archiveHashCache) chain(chainId string) *otter.Cache[common.Hash, struct{}] {
	if c == nil {
		return nil
	}
	v, ok := c.chains.Load(chainId)
	if !ok {
		return nil
	}
	return v.(*otter.Cache[common.Hash, struct{}])
}

// requestBlockHash returns the block hash the request is pinned to, if any.
func requestBlockHash(requestContext *types.RequestContext) (common.Hash, bool) {
	blockContext := requestContext.GetBlockContext()
	if blockContext == nil || blockContext.BlockId == nil || blockContext.BlockId.BlockHash == nil {
		return common.Hash{}, false
	}
	return *blockContext.BlockId.BlockHash, true
}
