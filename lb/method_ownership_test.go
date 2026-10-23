package lb

import (
	"context"
	"testing"
	"unsafe"

	ejrpc "github.com/Chaintable/nodex-proxy/jsonrpc"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/stretchr/testify/require"
)

// Metric series and trace attributes keep the method string for the process
// lifetime; it must not alias the decoded copy of a possibly large request body.
func TestGenerateRequestContextOwnsMethod(t *testing.T) {
	req := &protocol.Request{}
	req.SetBodyString(`{"jsonrpc":"2.0","method":"contractMultiCall","params":[],"id":1}`)
	rc := (&LoadBalancer{}).generateRequestContext(context.Background(), req)
	require.NoError(t, rc.Error)
	require.Equal(t, ejrpc.ContractMultiCall, rc.Method)
	require.NotEqual(t,
		uintptr(unsafe.Pointer(unsafe.StringData(string(rc.RequestBody[0].Method)))),
		uintptr(unsafe.Pointer(unsafe.StringData(string(rc.Method)))),
		"method must own its bytes")
}
