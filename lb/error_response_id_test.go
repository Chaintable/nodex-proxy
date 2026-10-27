package lb

import (
	"context"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/stretchr/testify/require"
)

func serveForTest(t *testing.T, lbi *LoadBalancer, chainID, body string) *app.RequestContext {
	t.Helper()
	c := app.NewContext(16)
	c.Request.SetRequestURI("http://test.local/" + chainID)
	c.Request.Header.SetMethod("POST")
	c.Request.Header.SetContentTypeBytes([]byte("application/json"))
	c.Request.SetBody([]byte(body))
	lbi.ServeHTTP(context.Background(), c, chainID)
	return c
}

func TestProxyErrorsEchoRequestID(t *testing.T) {
	lbi := newBenchLoadBalancer(t, []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`))

	// Validation error on a parsed single request.
	c := serveForTest(t, lbi, "1", `{"jsonrpc":"1.0","method":"eth_chainId","id":"abc"}`)
	require.Contains(t, string(c.Response.Body()), `"id":"abc"`)

	// Routing error: no backends for the chain.
	c = serveForTest(t, lbi, "424242", `{"jsonrpc":"2.0","method":"eth_chainId","id":7}`)
	require.Contains(t, string(c.Response.Body()), "no backends available")
	require.Contains(t, string(c.Response.Body()), `"id":7`)

	// Unparsable body: the id is unknown, keep the previous default.
	c = serveForTest(t, lbi, "1", `{`)
	require.Contains(t, string(c.Response.Body()), `"id":1`)
}
