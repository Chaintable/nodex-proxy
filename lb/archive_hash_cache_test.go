package lb

import (
	"context"
	"fmt"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/Chaintable/nodex-proxy/discovery"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/require"
)

func TestArchiveHashCacheBoundedPerChain(t *testing.T) {
	cache := newArchiveHashCache(64)
	for i := 0; i < 1000; i++ {
		cache.Add("1", crypto.Keccak256Hash([]byte{byte(i), byte(i >> 8)}))
	}
	chain := cache.chain("1")
	chain.CleanUp()
	require.LessOrEqual(t, chain.EstimatedSize(), 64)
	require.Nil(t, cache.chain("56"))
}

func TestArchiveHashCacheConcurrentAccess(t *testing.T) {
	cache := newArchiveHashCache(128)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				h := crypto.Keccak256Hash([]byte{byte(g), byte(i), byte(i >> 8)})
				cache.Add("1", h)
				cache.Contains("1", h)
			}
		}(g)
	}
	wg.Wait()
	chain := cache.chain("1")
	chain.CleanUp()
	require.LessOrEqual(t, chain.EstimatedSize(), 128)
}

func TestArchiveHashCachePerChainAndDisabled(t *testing.T) {
	require.Nil(t, newArchiveHashCache(0))
	require.Nil(t, newArchiveHashCache(-1))

	var disabled *archiveHashCache
	h := common.HexToHash("0x1")
	disabled.Add("1", h)
	require.False(t, disabled.HasChain("1"))
	require.False(t, disabled.Contains("1", h))

	cache := newArchiveHashCache(4)
	require.False(t, cache.HasChain("1"))
	cache.Add("1", h)
	require.True(t, cache.HasChain("1"))
	require.True(t, cache.Contains("1", h))
	require.False(t, cache.HasChain("56"))
	require.False(t, cache.Contains("56", h))
}

const archiveTestHash = "0x1111111111111111111111111111111111111111111111111111111111111111"

func blockHashRequest(hash string) string {
	return blockCtxRequest(hash, "Equals")
}

func blockCtxRequest(hash, blockType string) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","method":"debank_getBalance","params":["0x01",{"block_id":%q,"type":%q}],"id":1}`, hash, blockType)
}

func newArchiveCacheTestLB(t *testing.T, archiveBody string) (*LoadBalancer, *retryTestSelector) {
	t.Helper()
	stateSrv := httptest.NewServer(jsonRPCResponseHandler(`{"jsonrpc":"2.0","error":{"code":-39006,"message":"block not found for state node"},"id":1}`))
	t.Cleanup(stateSrv.Close)
	archiveSrv := httptest.NewServer(jsonRPCResponseHandler(archiveBody))
	t.Cleanup(archiveSrv.Close)

	sel := &retryTestSelector{
		state:   nodeFromTestServer(t, "state", stateSrv, discovery.NodeTypeState),
		archive: nodeFromTestServer(t, "archive", archiveSrv, discovery.NodeTypeArchive),
	}
	return &LoadBalancer{
		Config:             types.Config{DefaultRPCTimeout: 1000},
		NodeSelector:       sel,
		chainVersionRouter: NewChainVersionRouter(),
		archiveHashCache:   newArchiveHashCache(16),
	}, sel
}

func serveTestRequest(lb *LoadBalancer, chainID, body string) *app.RequestContext {
	c := app.NewContext(0)
	c.Request.SetRequestURI("http://proxy.local/" + chainID)
	c.Request.Header.SetMethod(consts.MethodPost)
	c.Request.Header.SetContentTypeBytes([]byte("application/json"))
	c.Request.SetBody([]byte(body))
	lb.ServeHTTP(context.Background(), c, chainID)
	return c
}

func TestServeHTTPRoutesCachedArchiveHashDirectly(t *testing.T) {
	const ok = `{"jsonrpc":"2.0","result":"0x1","id":1}`
	lb, sel := newArchiveCacheTestLB(t, ok)

	// First sight: state miss, archive retry, hash gets cached.
	c := serveTestRequest(lb, "1", blockHashRequest(archiveTestHash))
	require.JSONEq(t, ok, string(c.Response.Body()))
	require.Equal(t, []string{"state", "archive"}, sel.calls)

	// Same hash on the same chain skips the state node.
	sel.calls = nil
	c = serveTestRequest(lb, "1", blockHashRequest(archiveTestHash))
	require.JSONEq(t, ok, string(c.Response.Body()))
	require.Equal(t, []string{"archive"}, sel.calls)

	// The cache is per chain.
	sel.calls = nil
	serveTestRequest(lb, "56", blockHashRequest(archiveTestHash))
	require.Equal(t, []string{"state", "archive"}, sel.calls)

	// Other hashes on the cached chain still try state first.
	sel.calls = nil
	serveTestRequest(lb, "1", blockHashRequest("0x2222222222222222222222222222222222222222222222222222222222222222"))
	require.Equal(t, []string{"state", "archive"}, sel.calls)
}

func TestServeHTTPDoesNotCacheHashArchiveCannotServe(t *testing.T) {
	lb, sel := newArchiveCacheTestLB(t, `{"jsonrpc":"2.0","error":{"code":-39007,"message":"block is invalid"},"id":1}`)

	serveTestRequest(lb, "1", blockHashRequest(archiveTestHash))
	sel.calls = nil
	serveTestRequest(lb, "1", blockHashRequest(archiveTestHash))
	require.Equal(t, []string{"state", "archive"}, sel.calls)
	require.False(t, lb.archiveHashCache.HasChain("1"))
}

func TestServeHTTPDoesNotCacheFromBatch(t *testing.T) {
	lb, _ := newArchiveCacheTestLB(t, `[{"jsonrpc":"2.0","result":"0x1","id":1},{"jsonrpc":"2.0","result":"0x1","id":2}]`)

	single := blockHashRequest(archiveTestHash)
	serveTestRequest(lb, "1", "["+single+","+single+"]")
	require.False(t, lb.archiveHashCache.HasChain("1"))
}

func TestServeHTTPArchiveHashCacheDisabled(t *testing.T) {
	lb, sel := newArchiveCacheTestLB(t, `{"jsonrpc":"2.0","result":"0x1","id":1}`)
	lb.archiveHashCache = nil

	serveTestRequest(lb, "1", blockHashRequest(archiveTestHash))
	sel.calls = nil
	serveTestRequest(lb, "1", blockHashRequest(archiveTestHash))
	require.Equal(t, []string{"state", "archive"}, sel.calls)
}

func TestServeHTTPCachedArchiveFallsBackWhenNoArchiveNode(t *testing.T) {
	const ok = `{"jsonrpc":"2.0","result":"0x1","id":1}`
	lb, sel := newArchiveCacheTestLB(t, ok)
	serveTestRequest(lb, "1", blockHashRequest(archiveTestHash))

	sel.archive = nil // archive pool drained after the hash was cached
	sel.calls = nil
	serveTestRequest(lb, "1", blockHashRequest(archiveTestHash))
	require.Equal(t, []string{"archive", "state", "archive"}, sel.calls)
}

func TestServeHTTPContainsContextIgnoresArchiveHashCache(t *testing.T) {
	lb, sel := newArchiveCacheTestLB(t, `{"jsonrpc":"2.0","result":"0x1","id":1}`)
	serveTestRequest(lb, "1", blockHashRequest(archiveTestHash))
	require.True(t, lb.archiveHashCache.Contains("1", common.HexToHash(archiveTestHash)))

	// "Contains" is served from the node's latest state, so it must keep
	// going to state even when the same hash is cached as archive-only.
	sel.calls = nil
	serveTestRequest(lb, "1", blockCtxRequest(archiveTestHash, "Contains"))
	require.Equal(t, "state", sel.calls[0])
}

func TestServeHTTPBatchIgnoresArchiveHashCache(t *testing.T) {
	lb, sel := newArchiveCacheTestLB(t, `{"jsonrpc":"2.0","result":"0x1","id":1}`)
	serveTestRequest(lb, "1", blockHashRequest(archiveTestHash))

	sel.calls = nil
	single := blockHashRequest(archiveTestHash)
	serveTestRequest(lb, "1", "["+single+","+single+"]")
	require.Equal(t, "state", sel.calls[0])
}
