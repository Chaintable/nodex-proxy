package jsonrpc

import "testing"

func TestContainsErrorKey(t *testing.T) {
	cases := []struct {
		name string
		body string
		want bool
	}{
		{"error object", `{"jsonrpc":"2.0","error":{"code":-39006,"message":"state not found"},"id":1}`, true},
		{"capitalized key", `{"jsonrpc":"2.0","Error":{"code":-39006},"id":1}`, true},
		{"success", `{"jsonrpc":"2.0","result":"0x1234","id":1}`, false},
		{"batch with error", `[{"jsonrpc":"2.0","result":"0x1","id":1},{"jsonrpc":"2.0","error":{"code":-32000},"id":2}]`, true},
		// Quotes inside a JSON string value are escaped on the wire, so the
		// raw bytes contain \"error\" and never match the key token.
		{"quoted error text in result", `{"jsonrpc":"2.0","result":"literal \"error\" text","id":1}`, false},
		{"error word unquoted", `{"jsonrpc":"2.0","result":"an error occurred","id":1}`, false},
		// A nested key is an acceptable false positive: callers just fall
		// through to a full parse of the top-level object.
		{"nested error key", `{"jsonrpc":"2.0","result":{"error":null,"ok":true},"id":1}`, true},
		{"empty", ``, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ContainsErrorKey([]byte(tc.body)); got != tc.want {
				t.Fatalf("ContainsErrorKey(%q) = %v, want %v", tc.body, got, tc.want)
			}
		})
	}
}
