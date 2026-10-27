package lb

import (
	"testing"

	"github.com/Chaintable/nodex-proxy/internal/benchfixture"
	jrpc "github.com/Chaintable/nodex-proxy/lb/jsonrpc"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/cloudwego/hertz/pkg/app"
)

// Exercise both production consumers of the same upstream response, including
// the postprocessor's content-type and batch checks.
func BenchmarkResponseProcessing(b *testing.B) {
	config := types.DefaultConfig()
	processors := jrpc.GetPostProcessorHertz(&config)
	parseResponse := processors[0]
	for name, body := range benchfixture.Responses() {
		b.Run(name, func(b *testing.B) {
			lb := &LoadBalancer{}
			c := app.NewContext(0)
			c.Response.SetBody(body)
			c.Response.Header.SetContentType("application/json")
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				var p types.RequestContext
				lb.parseRPCErrorCodes(c.Response.Body(), &p)
				parseResponse(c, &p)
			}
		})
	}
}
