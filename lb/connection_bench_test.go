package lb

import (
	"context"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/Chaintable/nodex-proxy/discovery"
	"github.com/Chaintable/nodex-proxy/lb/lbnode"
	"github.com/cloudwego/hertz/pkg/app"
)

// Count actual upstream accepts/closes while using the production reverse
// proxy and shared HostClient. Rebuild Node objects to exercise pool reuse
// across discovery updates as well. Counts include the warmup connection and
// exclude server cleanup. This measures local reuse, not live cluster churn.
func BenchmarkUpstreamConnectionReuse(b *testing.B) {
	var opened, closed atomic.Int64
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	srv := &http.Server{
		ConnState: func(_ net.Conn, state http.ConnState) {
			if state == http.StateNew {
				opened.Add(1)
			}
			if state == http.StateClosed {
				closed.Add(1)
			}
		},
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`))
		}),
	}
	go func() { _ = srv.Serve(ln) }()
	b.Cleanup(func() { _ = srv.Close() })
	addr := ln.Addr().(*net.TCPAddr)
	var node *lbnode.Node
	c := app.NewContext(0)
	b.ResetTimer()
	for i := range b.N {
		if i%100 == 0 {
			node, err = lbnode.New("reuse-bench", addr.IP.String(), addr.Port, 100, discovery.NodeTypeState)
			if err != nil {
				b.Fatal(err)
			}
		}
		c.Request.SetRequestURI("http://proxy.local/1")
		c.Request.Header.SetMethod("POST")
		c.Request.SetBody([]byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`))
		c.Response.Reset()
		node.ReverseProxy.ServeHTTP(context.Background(), c)
		if c.Response.StatusCode() != 200 {
			b.Fatal(c.Response.StatusCode())
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(opened.Load()), "dials")
	b.ReportMetric(float64(closed.Load()), "closes")
}
