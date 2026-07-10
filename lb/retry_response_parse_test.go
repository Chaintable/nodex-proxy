package lb

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/Chaintable/nodex-proxy/discovery"
	"github.com/Chaintable/nodex-proxy/lb/lbnode"
	"github.com/Chaintable/nodex-proxy/lb/selector"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/stretchr/testify/require"
)

func TestParseRPCErrorCodes(t *testing.T) {
	lb := &LoadBalancer{}

	cases := []struct {
		name       string
		body       []byte
		wantState  bool
		wantNative bool
	}{
		{
			name:      "single state block not found",
			body:      []byte(`{"jsonrpc":"2.0","error":{"code":-39006,"message":"state block not found"},"id":1}`),
			wantState: true,
		},
		{
			name:       "batch with native retry code",
			body:       []byte(`[{"jsonrpc":"2.0","result":"0x1","id":1},{"jsonrpc":"2.0","error":{"code":-39008,"message":"cosmos precompile"},"id":2}]`),
			wantNative: true,
		},
		{
			name: "success response",
			body: []byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`),
		},
		{
			name: "invalid response",
			body: []byte(`not-json`),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			codes := lb.parseRPCErrorCodes(tc.body)
			require.Equal(t, tc.wantState, codes.Has(types.StateBlockNotFound))
			require.Equal(t, tc.wantNative, codes.Has(types.CosmosPrecompile))
		})
	}
}

func TestShouldRetryWithParsedErrorCodes(t *testing.T) {
	lb := &LoadBalancer{}
	codes := rpcErrorCodes{
		types.StateBlockNotFound: struct{}{},
		types.CosmosPrecompile:   struct{}{},
	}

	require.True(t, lb.shouldRetryWithArchive(&types.RequestContext{}, codes))
	require.False(t, lb.shouldRetryWithArchive(&types.RequestContext{Archive: true}, codes))
	require.False(t, lb.shouldRetryWithArchive(nil, codes))

	require.True(t, lb.shouldRetryWithNative(&types.RequestContext{}, codes))
	require.False(t, lb.shouldRetryWithNative(&types.RequestContext{Native: true}, codes))
	require.False(t, lb.shouldRetryWithNative(nil, codes))
}

func TestServeHTTPRetriesArchiveThenNativeFromParsedErrorCodes(t *testing.T) {
	stateSrv := httptest.NewServer(jsonRPCResponseHandler(`{"jsonrpc":"2.0","error":{"code":-39006,"message":"state block not found"},"id":1}`))
	defer stateSrv.Close()
	archiveSrv := httptest.NewServer(jsonRPCResponseHandler(`{"jsonrpc":"2.0","error":{"code":-39008,"message":"cosmos precompile"},"id":1}`))
	defer archiveSrv.Close()
	nativeSrv := httptest.NewServer(jsonRPCResponseHandler(`{"jsonrpc":"2.0","result":"0x1","id":1}`))
	defer nativeSrv.Close()

	selector := &retryTestSelector{
		state:   nodeFromTestServer(t, "state", stateSrv, discovery.NodeTypeState),
		archive: nodeFromTestServer(t, "archive", archiveSrv, discovery.NodeTypeArchive),
		native:  nodeFromTestServer(t, "native", nativeSrv, discovery.NodeTypeState, lbnode.WithSource("native")),
	}
	lb := &LoadBalancer{
		Config:             types.Config{DefaultRPCTimeout: 1000},
		NodeSelector:       selector,
		chainVersionRouter: NewChainVersionRouter(),
	}

	c := app.NewContext(0)
	c.Request.SetRequestURI("http://proxy.local/1")
	c.Request.Header.SetMethod(consts.MethodPost)
	c.Request.Header.SetContentTypeBytes([]byte("application/json"))
	c.Request.SetBody([]byte(`{"jsonrpc":"2.0","method":"contractMultiCall","params":[],"id":1}`))

	lb.ServeHTTP(context.Background(), c, "1")

	require.Equal(t, []string{"state", "archive", "native"}, selector.calls)
	require.JSONEq(t, `{"jsonrpc":"2.0","result":"0x1","id":1}`, string(c.Response.Body()))
}

func jsonRPCResponseHandler(body string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	})
}

func nodeFromTestServer(t *testing.T, key string, srv *httptest.Server, nodeType discovery.NodeType, opts ...lbnode.Option) *lbnode.Node {
	t.Helper()

	u, err := url.Parse(srv.URL)
	require.NoError(t, err)
	host, rawPort, err := net.SplitHostPort(u.Host)
	require.NoError(t, err)
	port, err := strconv.Atoi(rawPort)
	require.NoError(t, err)

	node, err := lbnode.New(key, host, port, types.DefaultWeight, nodeType, opts...)
	require.NoError(t, err)
	return node
}

type retryTestSelector struct {
	state   *lbnode.Node
	archive *lbnode.Node
	native  *lbnode.Node
	calls   []string
}

func (s *retryTestSelector) String() string {
	return "retry-test-selector"
}

func (s *retryTestSelector) GetNode(ctx *types.RequestContext, requestKey string) (*lbnode.Node, error) {
	switch {
	case requestKey == "native" || (ctx != nil && ctx.Native):
		s.calls = append(s.calls, "native")
		return s.native, nil
	case ctx != nil && ctx.Archive:
		s.calls = append(s.calls, "archive")
		return s.archive, nil
	default:
		s.calls = append(s.calls, "state")
		return s.state, nil
	}
}

func (s *retryTestSelector) UpsertNode(context.Context, string, discovery.NodeType, *lbnode.Node) error {
	return nil
}

func (s *retryTestSelector) RemoveNode(context.Context, string, discovery.NodeType, *lbnode.Node) error {
	return nil
}

func (s *retryTestSelector) UpdateChainHeight(context.Context, string, *hexutil.Big) error {
	return nil
}

func (s *retryTestSelector) GetAllNodes(string) ([]*lbnode.Node, bool) {
	return []*lbnode.Node{s.state, s.archive, s.native}, true
}

func (s *retryTestSelector) GetArchiveNodes(string) ([]*lbnode.Node, bool) {
	return []*lbnode.Node{s.archive}, true
}

func (s *retryTestSelector) GetStateNodes(string) ([]*lbnode.Node, bool) {
	return []*lbnode.Node{s.state}, true
}

func (s *retryTestSelector) GetNativeNodes(string) ([]*lbnode.Node, bool) {
	return []*lbnode.Node{s.native}, true
}

func (s *retryTestSelector) GetAllChainsIDs() []string {
	return []string{"1"}
}

var _ selector.Strategy = (*retryTestSelector)(nil)
