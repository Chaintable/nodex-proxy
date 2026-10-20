package lb

import (
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/Chaintable/nodex-proxy/types"
	"github.com/ethereum/go-ethereum/common"
)

// archiveHashCache remembers, per chain, block hashes that state nodes could
// not serve (-39006) but archive nodes could. A state node only keeps a short
// window of recent blocks in memory and cannot tell how old a hash is, so a
// hash-addressed historical request normally costs a wasted state hop before
// the archive retry. Hits on this cache go straight to archive instead.
//
// The cache is lossy by design: a miss only costs the old state-then-archive
// path, so lookups are lock-free and never write shared memory, and eviction
// is approximate FIFO rather than strict LRU.
//
// A nil *archiveHashCache is a valid, disabled cache.
type archiveHashCache struct {
	size   int
	chains sync.Map // chainId -> *hashSet
}

func newArchiveHashCache(size int) *archiveHashCache {
	if size <= 0 {
		return nil
	}
	return &archiveHashCache{size: size}
}

// Contains reports whether hash is known to need an archive node on chainId.
func (c *archiveHashCache) Contains(chainId string, hash common.Hash) bool {
	s := c.chain(chainId)
	if s == nil {
		return false
	}
	return s.Contains(hash)
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
		v, _ = c.chains.LoadOrStore(chainId, newHashSet(c.size))
	}
	v.(*hashSet).Add(hash)
}

func (c *archiveHashCache) chain(chainId string) *hashSet {
	if c == nil {
		return nil
	}
	v, ok := c.chains.Load(chainId)
	if !ok {
		return nil
	}
	return v.(*hashSet)
}

// hashSetWays is the bucket associativity: 8 uint64 slots fill one 64-byte
// cache line, so a lookup touches a single line.
const hashSetWays = 8

// hashSet is a lock-free, fixed-capacity, set-associative set of block hash
// fingerprints. Block hashes are keccak outputs, so their first 8 bytes are
// uniformly distributed and serve as both bucket index and fingerprint. A
// fingerprint collision (~2^-60 per lookup) only sends a request to archive,
// which can serve any block. Each bucket evicts in insertion order (FIFO) via
// a write-side cursor, so reads do no bookkeeping at all.
type hashSet struct {
	buckets []hashBucket
	cursors []atomic.Uint32 // per-bucket next slot to overwrite; write path only
	mask    uint64
}

type hashBucket [hashSetWays]atomic.Uint64

func newHashSet(size int) *hashSet {
	n := 1
	for n*hashSetWays < size {
		n <<= 1
	}
	return &hashSet{
		buckets: make([]hashBucket, n),
		cursors: make([]atomic.Uint32, n),
		mask:    uint64(n - 1),
	}
}

// fingerprint is never zero; zero marks an empty slot.
func fingerprint(hash common.Hash) uint64 {
	return binary.LittleEndian.Uint64(hash[:8]) | 1
}

// bucketIndex skips the low bit, which fingerprint forces to 1.
func (s *hashSet) bucketIndex(fp uint64) uint64 {
	return (fp >> 1) & s.mask
}

func (s *hashSet) Contains(hash common.Hash) bool {
	fp := fingerprint(hash)
	b := &s.buckets[s.bucketIndex(fp)]
	for i := range b {
		if b[i].Load() == fp {
			return true
		}
	}
	return false
}

func (s *hashSet) Add(hash common.Hash) {
	fp := fingerprint(hash)
	idx := s.bucketIndex(fp)
	b := &s.buckets[idx]
	for i := range b {
		if b[i].Load() == fp {
			return
		}
	}
	// Slots fill in cursor order, so the cursor always points at the oldest
	// entry. Racing writers may duplicate or drop an entry; the worst outcome
	// is a later miss, which falls back to state first.
	slot := (s.cursors[idx].Add(1) - 1) % hashSetWays
	b[slot].Store(fp)
}

// Len counts occupied slots. O(capacity); for tests and diagnostics only.
func (s *hashSet) Len() int {
	n := 0
	for i := range s.buckets {
		for j := range s.buckets[i] {
			if s.buckets[i][j].Load() != 0 {
				n++
			}
		}
	}
	return n
}

// requestBlockHash returns the block hash the request is pinned to, if any.
func requestBlockHash(requestContext *types.RequestContext) (common.Hash, bool) {
	blockContext := requestContext.GetBlockContext()
	if blockContext == nil || blockContext.BlockId == nil || blockContext.BlockId.BlockHash == nil {
		return common.Hash{}, false
	}
	return *blockContext.BlockId.BlockHash, true
}
