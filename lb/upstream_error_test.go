package lb

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Chaintable/nodex-proxy/discovery"
	ejrpc "github.com/Chaintable/nodex-proxy/jsonrpc"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/stretchr/testify/require"
)

func TestServeHTTPMapsUpstreamErrors(t *testing.T) {
	cases := []struct {
		name       string
		handler    http.HandlerFunc
		closed     bool
		wantStatus int
		wantCode   ejrpc.ErrorCode
		wantData   string
	}{
		{
			name: "no response before the read timeout",
			handler: func(w http.ResponseWriter, r *http.Request) {
				waitForCancel(r)
			},
			wantStatus: consts.StatusGatewayTimeout,
			wantCode:   ejrpc.ExtWaitingTargetResponseTimeout,
			wantData:   "reverse proxy gateway timeout",
		},
		{
			name: "body stalls after the headers",
			handler: func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Content-Length", "64")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"jsonrpc":"2.0",`))
				w.(http.Flusher).Flush()
				waitForCancel(r)
			},
			wantStatus: consts.StatusGatewayTimeout,
			wantCode:   ejrpc.ExtWaitingTargetResponseTimeout,
			wantData:   "reverse proxy gateway timeout",
		},
		{
			name:       "backend unreachable",
			handler:    func(http.ResponseWriter, *http.Request) {},
			closed:     true,
			wantStatus: consts.StatusBadGateway,
			wantCode:   ejrpc.InternalErrorCode,
			wantData:   "reverse proxy bad gateway",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(tc.handler)
			node := nodeFromTestServer(t, "state", srv, discovery.NodeTypeState)
			if tc.closed {
				srv.Close()
			} else {
				defer srv.Close()
			}

			lb := &LoadBalancer{
				Config:             types.Config{DefaultRPCTimeout: 200},
				NodeSelector:       &retryTestSelector{state: node},
				chainVersionRouter: NewChainVersionRouter(),
			}

			c := app.NewContext(0)
			c.Request.SetRequestURI("http://proxy.local/1")
			c.Request.Header.SetMethod(consts.MethodPost)
			c.Request.Header.SetContentTypeBytes([]byte("application/json"))
			c.Request.SetBody([]byte(`{"jsonrpc":"2.0","method":"eth_call","params":[],"id":1}`))

			lb.ServeHTTP(context.Background(), c, "1")

			require.Equal(t, tc.wantStatus, c.Response.StatusCode())
			var resp struct {
				Error struct {
					Code ejrpc.ErrorCode `json:"code"`
					Data string          `json:"data"`
				} `json:"error"`
			}
			require.NoError(t, json.Unmarshal(c.Response.Body(), &resp), string(c.Response.Body()))
			require.Equal(t, tc.wantCode, resp.Error.Code)
			require.Equal(t, tc.wantData, resp.Error.Data)
		})
	}
}

// waitForCancel holds the response open until the proxy gives up on it, with
// an upper bound so a regression cannot hang the test.
func waitForCancel(r *http.Request) {
	// net/http only notices the peer closing the connection once the request
	// body has been consumed.
	_, _ = io.Copy(io.Discard, r.Body)
	select {
	case <-r.Context().Done():
	case <-time.After(5 * time.Second):
	}
}
