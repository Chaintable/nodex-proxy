package jsonrpc

import (
	"testing"

	"github.com/Chaintable/nodex-proxy/internal/benchfixture"
	"github.com/bytedance/sonic"
)

// preownedString is a lower bound: the input string is prepared outside the
// timer. Hertz supplies pooled []byte, so this is not a safe drop-in production
// optimization and its preparation cost must not be treated as free.
func BenchmarkRequestDecodeOwnership(b *testing.B) {
	for name, body := range benchfixture.Requests() {
		if name == "batch" {
			continue
		}
		owned := string(body)
		for strategy, decode := range map[string]func(*RequestObject) error{
			"bytes":          func(req *RequestObject) error { return sonic.Unmarshal(body, req) },
			"preownedString": func(req *RequestObject) error { return sonic.UnmarshalString(owned, req) },
		} {
			b.Run(name+"/"+strategy, func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					var req RequestObject
					if err := decode(&req); err != nil {
						b.Fatal(err)
					}
					benchmarkRequests = []*RequestObject{&req}
				}
			})
		}
	}
}
