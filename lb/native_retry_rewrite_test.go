package lb

import (
	"testing"

	ejrpc "github.com/Chaintable/nodex-proxy/jsonrpc"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/bytedance/sonic"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/stretchr/testify/require"
)

func TestRewriteMethodForNativeRetry_Single(t *testing.T) {
	lb := &LoadBalancer{}
	c := app.NewContext(0)

	reqObj := &ejrpc.RequestObject{
		Jsonrpc: "2.0",
		Method:  ejrpc.SimulateTransactions,
		Params:  sonic.NoCopyRawMessage(`[]`),
		ID:      sonic.NoCopyRawMessage(`1`),
	}
	origBody, err := sonic.Marshal(reqObj)
	require.NoError(t, err)
	c.Request.SetBody(origBody)

	rctx := &types.RequestContext{
		RawRequestBody:  origBody,
		RequestBody:     []*ejrpc.RequestObject{reqObj},
		RequestBodySize: len(origBody),
		IsBatch:         false,
		Method:          reqObj.Method,
	}

	lb.rewriteMethodForNativeRetry(c, rctx)

	require.Equal(t, ejrpc.RPCMethod("debank_simulateTransactions"), rctx.RequestBody[0].Method)
	require.Equal(t, ejrpc.RPCMethod("debank_simulateTransactions"), rctx.Method)

	var got ejrpc.RequestObject
	require.NoError(t, sonic.Unmarshal(c.Request.Body(), &got))
	require.Equal(t, ejrpc.RPCMethod("debank_simulateTransactions"), got.Method)
}

func TestRewriteMethodForNativeRetry_Batch(t *testing.T) {
	lb := &LoadBalancer{}
	c := app.NewContext(0)

	reqs := []*ejrpc.RequestObject{
		{Jsonrpc: "2.0", Method: ejrpc.ContractMultiCall, Params: sonic.NoCopyRawMessage(`[]`), ID: sonic.NoCopyRawMessage(`1`)},
		{Jsonrpc: "2.0", Method: ejrpc.EstimateGas, Params: sonic.NoCopyRawMessage(`[]`), ID: sonic.NoCopyRawMessage(`2`)},
		{Jsonrpc: "2.0", Method: ejrpc.SimulateTransactions, Params: sonic.NoCopyRawMessage(`[]`), ID: sonic.NoCopyRawMessage(`3`)},
		{Jsonrpc: "2.0", Method: ejrpc.RPCMethod("eth_blockNumber"), Params: sonic.NoCopyRawMessage(`[]`), ID: sonic.NoCopyRawMessage(`4`)},
	}

	origBody, err := sonic.Marshal(reqs)
	require.NoError(t, err)
	c.Request.SetBody(origBody)

	rctx := &types.RequestContext{
		RawRequestBody:  origBody,
		RequestBody:     reqs,
		RequestBodySize: len(origBody),
		IsBatch:         true,
	}

	lb.rewriteMethodForNativeRetry(c, rctx)

	require.Equal(t, ejrpc.RPCMethod("debank_contractMultiCall"), rctx.RequestBody[0].Method)
	require.Equal(t, ejrpc.RPCMethod("debank_estimateGas"), rctx.RequestBody[1].Method)
	require.Equal(t, ejrpc.RPCMethod("debank_simulateTransactions"), rctx.RequestBody[2].Method)
	require.Equal(t, ejrpc.RPCMethod("eth_blockNumber"), rctx.RequestBody[3].Method)

	var got []*ejrpc.RequestObject
	require.NoError(t, sonic.Unmarshal(c.Request.Body(), &got))
	require.Len(t, got, 4)
	require.Equal(t, ejrpc.RPCMethod("debank_contractMultiCall"), got[0].Method)
	require.Equal(t, ejrpc.RPCMethod("debank_estimateGas"), got[1].Method)
	require.Equal(t, ejrpc.RPCMethod("debank_simulateTransactions"), got[2].Method)
	require.Equal(t, ejrpc.RPCMethod("eth_blockNumber"), got[3].Method)
}

func TestRewriteMethodForNativeRetry_NoChange(t *testing.T) {
	lb := &LoadBalancer{}
	c := app.NewContext(0)

	reqObj := &ejrpc.RequestObject{
		Jsonrpc: "2.0",
		Method:  ejrpc.RPCMethod("eth_blockNumber"),
		Params:  sonic.NoCopyRawMessage(`[]`),
		ID:      sonic.NoCopyRawMessage(`1`),
	}
	origBody, err := sonic.Marshal(reqObj)
	require.NoError(t, err)
	c.Request.SetBody(origBody)

	rctx := &types.RequestContext{
		RawRequestBody:  origBody,
		RequestBody:     []*ejrpc.RequestObject{reqObj},
		RequestBodySize: len(origBody),
		IsBatch:         false,
		Method:          reqObj.Method,
	}

	lb.rewriteMethodForNativeRetry(c, rctx)

	require.Equal(t, origBody, c.Request.Body())
	require.Equal(t, ejrpc.RPCMethod("eth_blockNumber"), rctx.RequestBody[0].Method)
	require.Equal(t, ejrpc.RPCMethod("eth_blockNumber"), rctx.Method)
}
