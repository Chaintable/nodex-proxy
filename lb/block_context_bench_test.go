package lb

import (
	"strings"
	"testing"

	"github.com/Chaintable/nodex-proxy/internal/benchfixture"
	ejrpc "github.com/Chaintable/nodex-proxy/jsonrpc"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/bytedance/sonic"
)

var benchmarkBlockContext *types.BlockContext

func BenchmarkBlockContextLargeParams(b *testing.B) {
	fixtures := make(map[string][]byte)
	for name, size := range map[string]int{"small": 32, "4KiB": 4096, "40KiB": 40960, "256KiB": 262144} {
		fixtures[name] = []byte(`[["0x` + strings.Repeat("ab", size/2) + `"],{"block_id":"0x1234","type":"Equals"}]`)
	}
	var multicall ejrpc.RequestObject
	if err := sonic.Unmarshal(benchfixture.Requests()["multicall"], &multicall); err != nil {
		b.Fatal(err)
	}
	fixtures["calls128"] = multicall.Params
	for name, params := range fixtures {
		requests := []*ejrpc.RequestObject{{Method: ejrpc.ContractMultiCall, Params: params}}
		for strategy, parse := range map[string]func([]*ejrpc.RequestObject) *types.BlockContext{
			"production": (&LoadBalancer{}).ParseBlockContext,
		} {
			b.Run(name+"/"+strategy, func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(params)))
				for b.Loop() {
					benchmarkBlockContext = parse(requests)
					if benchmarkBlockContext == nil {
						b.Fatal("missing block context")
					}
				}
			})
		}
	}
}

// Include generic last-argument routing and batch fallthrough, not just the
// large second-argument path. Compare the same benchmark on separate revisions.
func BenchmarkBlockContextSelection(b *testing.B) {
	ctx := `{"block_id":"0x1234","type":"Equals"}`
	for name, requests := range map[string][]*ejrpc.RequestObject{
		"genericSmall":     {{Method: "generic", Params: []byte(`[null,` + ctx + `]`)}},
		"getBalanceLatest": {{Method: "eth_getBalance", Params: []byte(`["0x1234","latest"]`)}},
		"ethCall4KiB":      {{Method: "eth_call", Params: []byte(`[{"to":"0x2222222222222222222222222222222222222222","data":"0x` + strings.Repeat("ab", 2048) + `"},` + ctx + `]`)}},
		"genericLarge":     {{Method: "generic", Params: []byte(`[["0x` + strings.Repeat("ab", 20480) + `"],` + ctx + `]`)}},
		"defaultLatest":    {{Method: ejrpc.ContractMultiCall, Params: []byte(`[null]`)}},
		"batchFallthrough": {{Method: ejrpc.ContractMultiCall, Params: []byte(`[null,"latest"]`)}, {Method: ejrpc.ContractMultiCall, Params: []byte(`[null,` + ctx + `]`)}},
	} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				benchmarkBlockContext = (&LoadBalancer{}).ParseBlockContext(requests)
			}
		})
	}
}
