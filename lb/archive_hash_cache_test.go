package lb

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/Chaintable/nodex-proxy/discovery"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
)

func TestHashLRUEvictsLeastRecentlyUsed(t *testing.T) {
	l := newHashLRU(2)
	a, b, c := common.HexToHash("0xa"), common.HexToHash("0xb"), common.HexToHash("0xc")

	l.Add(a)
	l.Add(b)
	require.True(t, l.Get(a)) // a becomes most recent, b is now oldest
	l.Add(c)

	require.Equal(t, 2, l.Len())
	require.True(t, l.Get(a))
	require.False(t, l.Get(b))
	require.True(t, l.Get(c))

	l.Add(c) // re-adding an existing hash must not grow the set
	require.Equal(t, 2, l.Len())
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
	return fmt.Sprintf(`{"jsonrpc":"2.0","method":"debank_getBalance","params":["0x01",{"block_id":%q,"type":"Equals"}],"id":1}`, hash)
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
