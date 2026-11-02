package random

import (
	"encoding/binary"
	"math"

	"github.com/Chaintable/nodex-proxy/lb/lbnode"
	"github.com/Chaintable/nodex-proxy/types"
)

// blockKey returns the routing key of a request pinned to one block: its hash,
// or its number. Requests that follow the head ("latest", "Contains") have no
// fixed block and get no key.
func blockKey(blockContext *types.BlockContext) (uint64, bool) {
	if blockContext == nil || blockContext.Type != "Equals" || blockContext.BlockId == nil {
		return 0, false
	}
	if hash := blockContext.BlockId.BlockHash; hash != nil {
		// A block hash is already uniform; its first 8 bytes are enough.
		return binary.LittleEndian.Uint64(hash[:8]), true
	}
	if number := blockContext.BlockId.BlockNumber; number != nil && *number >= 0 {
		return mix64(uint64(*number)), true
	}
	return 0, false
}

// pickByBlock chooses the node for a block by weighted rendezvous hashing.
// Every proxy replica sends the requests on one block to the same archive
// node, so the reads they share are served from that node's caches instead
// of being loaded on every node. Adding or removing a node only moves the
// blocks it gains or owned. weight must be positive for at least one node.
func pickByBlock(key uint64, nodes []*lbnode.Node, weight func(string) int) *lbnode.Node {
	var best *lbnode.Node
	bestScore := 0.0
	for _, node := range nodes {
		w := weight(node.Key())
		if w <= 0 {
			continue
		}
		h := mix64(key ^ stringHash(node.Key()))
		// u is uniform in (0, 1); w / -ln(u) gives each node a share of keys
		// proportional to its weight.
		u := (float64(h>>11) + 0.5) / (1 << 53)
		score := float64(w) / -math.Log(u)
		if best == nil || score > bestScore {
			best, bestScore = node, score
		}
	}
	return best
}

// stringHash is 64-bit FNV-1a. It must be the same in every proxy replica,
// so it cannot use a per-process seed.
func stringHash(s string) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

// mix64 is the splitmix64 finalizer.
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}
