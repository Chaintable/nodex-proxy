package jsonrpc

import (
	"context"
	"testing"
	"time"

	"github.com/Chaintable/nodex-proxy/types"
	"github.com/cloudwego/hertz/pkg/app"
)

func BenchmarkUpdatePostProcessorMetricsHertz(b *testing.B) {
	for _, batch := range []bool{false, true} {
		name := "single"
		if batch {
			name = "batch"
		}
		b.Run(name, func(b *testing.B) {
			c := app.NewContext(0)
			c.Response.SetStatusCode(200)
			c.Response.SetBody([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x1234"}`))
			p := &types.RequestContext{
				Host: "bench.local", Target: "native", BaseChainId: "1", ChainId: "1-v1", ChainUUID: "v1",
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
