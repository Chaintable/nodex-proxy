package jsonrpc

import (
	"bytes"
	"testing"

	"github.com/Chaintable/nodex-proxy/internal/benchfixture"
	"github.com/bytedance/sonic"
	"github.com/bytedance/sonic/ast"
	"github.com/cloudwego/hertz/pkg/protocol"
)

var benchmarkContains bool
var benchmarkRequests []*RequestObject

func BenchmarkContainsErrorKey(b *testing.B) {
	for name, body := range benchfixture.Responses() {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(body)))
			for b.Loop() {
				benchmarkContains = ContainsErrorKey(body)
			}
		})
	}
}

// Compare alternatives on the same immutable fixtures. The narrow struct
// inspects only top-level error members and skips result values with Sonic.
// It is an evaluation candidate, not a replacement for the compatible filter:
// malformed JSON and escaped/case-folded keys have different semantics.
func BenchmarkResponseDetectionAlternatives(b *testing.B) {
	for name, body := range benchfixture.Responses() {
		for strategy, detect := range map[string]func([]byte) bool{
			"legacyTwoScans": func(body []byte) bool {
				return bytes.Contains(body, []byte(`"error"`)) || bytes.Contains(body, []byte(`"Error"`))
			},
			"topLevelSonic": func(body []byte) bool {
				type response struct {
					Error sonic.NoCopyRawMessage `json:"error"`
				}
				if IsBatch(body) {
					var responses []response
					if sonic.Unmarshal(body, &responses) != nil {
						return false
					}
					for _, r := range responses {
						if len(r.Error) != 0 {
							return true
						}
					}
					return false
				}
				var r response
				return sonic.Unmarshal(body, &r) == nil && len(r.Error) != 0
			},
		} {
			b.Run(name+"/"+strategy, func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					benchmarkContains = detect(body)
				}
			})
		}
	}
}

func BenchmarkParseRequest(b *testing.B) {
	for name, body := range benchfixture.Requests() {
		b.Run(name, func(b *testing.B) {
			var req protocol.Request
			req.SetBody(body)
			b.ReportAllocs()
			b.SetBytes(int64(len(body)))
			for b.Loop() {
				_, parsed, _, err := ParseRequest(&req)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkRequests = parsed
			}
		})
	}
}

// Evaluation only: build the fields needed by routing, validation, native
// retry rewriting, and request logs using Sonic's lazy AST. This candidate
// deliberately stays out of production: exact-key lookup also differs from
// Unmarshal's handling of case variants and duplicate keys.
func BenchmarkParseRequestOnDemand(b *testing.B) {
	readObject := func(node *ast.Node) (*RequestObject, error) {
		version, err := node.Get("jsonrpc").String()
		if err != nil {
			return nil, err
		}
		method, err := node.Get("method").String()
		if err != nil {
			return nil, err
		}
		params, err := node.Get("params").Raw()
		if err != nil {
			return nil, err
		}
		id, err := node.Get("id").Raw()
		if err != nil {
			return nil, err
		}
		return &RequestObject{Jsonrpc: version, Method: RPCMethod(method), Params: []byte(params), ID: []byte(id)}, nil
	}
	for name, body := range benchfixture.Requests() {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				root, err := sonic.GetWithOptions(body, ast.SearchOptions{ValidateJSON: true})
				if err != nil {
					b.Fatal(err)
				}
				var requests []*RequestObject
				if IsBatch(body) {
					values, err := root.Values()
					if err != nil {
						b.Fatal(err)
					}
					var node ast.Node
					for values.Next(&node) {
						req, err := readObject(&node)
						if err != nil {
							b.Fatal(err)
						}
						requests = append(requests, req)
					}
				} else {
					req, err := readObject(&root)
					if err != nil {
						b.Fatal(err)
					}
					requests = []*RequestObject{req}
				}
				benchmarkRequests = requests
			}
		})
	}
}
