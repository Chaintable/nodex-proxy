package jsonrpc

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Chaintable/nodex-proxy/types"
	"github.com/cloudwego/hertz/pkg/app"
)

func BenchmarkUpdatePostProcessorMetricsHertz(b *testing.B) {
	// An oversized host is never cached: the path every new tuple takes once the
	// handle cache is full.
	for _, tc := range []struct {
		name  string
		batch bool
		host  types.ProcessorHost
	}{
		{"single", false, "bench.local"},
		{"batch", true, "bench.local"},
		{"uncached", false, types.ProcessorHost(strings.Repeat("h", 1100))},
	} {
		batch := tc.batch
		b.Run(tc.name, func(b *testing.B) {
			c := app.NewContext(0)
			c.Response.SetStatusCode(200)
			c.Response.SetBody([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x1234"}`))
			p := &types.RequestContext{
				Host: tc.host, Target: "native", BaseChainId: "1", ChainId: "1-v1", ChainUUID: "v1",
				Method: "contractMultiCall", IsBatch: batch, RequestBodySize: 40000, Start: time.Now(),
			}
			update := updatePostProcessorMetricsHertz()
			update(context.Background(), c, p)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				update(context.Background(), c, p)
			}
		})
	}
}
