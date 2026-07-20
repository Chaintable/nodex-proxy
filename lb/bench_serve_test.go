package lb

import (
	"context"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/Chaintable/nodex-proxy/discovery"
	"github.com/Chaintable/nodex-proxy/lb/jsonrpc"
	"github.com/Chaintable/nodex-proxy/lb/lbnode"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

// startMockUpstream starts a trivial upstream JSON-RPC server that always
// returns resp, so benchmarks exercise the full proxy path against a real
// local HTTP round trip.
func startMockUpstream(tb testing.TB, resp []byte) (string, int) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(resp)
	})}
	go func() { _ = srv.Serve(ln) }()
	tb.Cleanup(func() { _ = srv.Close() })
	addr := ln.Addr().(*net.TCPAddr)
	return addr.IP.String(), addr.Port
}

func newBenchLoadBalancer(tb testing.TB, upstreamResp []byte) *LoadBalancer {
	cfg := types.DefaultConfig()
	cfg.Processor.MethodNameChecker.Enable = true
	heightMap := jsonrpc.NewHeightMap()
	lbi := NewLoadBalancer(
		context.Background(),
		nil, cfg,
		&jsonrpc.GeneralRPCMethodHertzHandler{Config: &cfg, HeightMap: heightMap},
		jsonrpc.NewMethodLimiter(nil), heightMap, jsonrpc.NewMirrorLimiter(),
		make(chan *discovery.TargetNode), make(chan *discovery.ChainHeight),
		make(chan *discovery.Gateway), make(chan *discovery.MirrorTarget),
		make(chan *discovery.ChainVersion),
	)

	ip, port := startMockUpstream(tb, upstreamResp)
	node, err := lbnode.New(fmt.Sprintf("bench_%s_%d", ip, port), ip, port, 100, discovery.NodeTypeState)
	if err != nil {
		tb.Fatal(err)
	}
	node.SetState(1) // 1 = available (see lbnode.Node.Available)
	if err := lbi.NodeSelector.UpsertNode(context.Background(), "1", discovery.NodeTypeState, node); err != nil {
		tb.Fatal(err)
	}
	height := hexutil.Big(*big.NewInt(20_000_000))
	_ = lbi.NodeSelector.UpdateChainHeight(context.Background(), "1", &height)
	heightMap.SetHeight("1", &height)
	return lbi
}

func benchServe(b *testing.B, requestBody, upstreamResp []byte) {
	lbi := newBenchLoadBalancer(b, upstreamResp)

	// warm up the connection pool and sanity-check the wiring
	c := app.NewContext(16)
	c.Request.SetRequestURI("http://bench.local/1")
	c.Request.Header.SetMethod("POST")
	c.Request.Header.SetContentTypeBytes([]byte("application/json"))
	c.Request.SetBody(requestBody)
	lbi.ServeHTTP(context.Background(), c, "1")
	if c.Response.StatusCode() != 200 {
		b.Fatalf("warmup failed: status=%d body=%s", c.Response.StatusCode(), c.Response.Body())
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c := app.NewContext(16)
			c.Request.SetRequestURI("http://bench.local/1")
			c.Request.Header.SetMethod("POST")
			c.Request.Header.SetContentTypeBytes([]byte("application/json"))
			c.Request.SetBody(requestBody)
			lbi.ServeHTTP(context.Background(), c, "1")
			if c.Response.StatusCode() != 200 {
				b.Fatalf("unexpected status %d: %s", c.Response.StatusCode(), c.Response.Body())
			}
		}
	})
}

func BenchmarkServeHTTPSmall(b *testing.B) {
	requestBody := []byte(`{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x0cecb15396825A895FF8DA8fC2D2C77A234dcCee","latest"],"id":1}`)
	upstreamResp := []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1234567890abcdef"}`)
	benchServe(b, requestBody, upstreamResp)
}

func BenchmarkServeHTTPLargeResp(b *testing.B) {
	requestBody := []byte(`{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock":"0x112a880","toBlock":"latest","address":"0x0cecb15396825A895FF8DA8fC2D2C77A234dcCee"}],"id":1}`)
	// ~64KB response, shaped like a real eth_getLogs result
	entry := `{"address":"0x0cecb15396825a895ff8da8fc2d2c77a234dccee","topics":["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],"data":"0x00000000000000000000000000000000000000000000000000000001a13b8600","blockNumber":"0x112a884","transactionHash":"0x88df016429689c079f3b2f6ad39fa052532c56795b733da78a91ebe6a713944b","transactionIndex":"0x1","blockHash":"0x8243343df08b9751f5ca0c5f8c9c0460d8a9b6351066fae0acbd4d3e776de8bb","logIndex":"0x0","removed":false}`
	var sb strings.Builder
	sb.WriteString(`{"jsonrpc":"2.0","id":1,"result":[`)
	for i := range 128 {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(entry)
	}
	sb.WriteString(`]}`)
	benchServe(b, requestBody, []byte(sb.String()))
}
