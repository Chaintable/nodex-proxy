package jsonrpc

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/Chaintable/nodex-proxy/internal/benchfixture"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/stretchr/testify/require"
)

var parseRequestSeeds = []string{
	`{"jsonrpc":"2.0","method":"contractMultiCall","params":[[{"to":"0x01","data":"0x02"}],{"block_id":"0x1234","type":"Equals"}],"id":1}`,
	`{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x1234","latest"],"id":"abc"}`,
	` { "id" : 7 , "params" : [ 1 , 2 ] , "method" : "m" , "jsonrpc" : "2.0" } `,
	`{"jsonrpc":"2.0","method":"m","params":null,"id":null}`,
	`{"jsonrpc":"2.0","method":"m"}`,
	`{"jsonrpc":"2.0","Method":"m","id":1}`,
	`{"jsonrpc":"2.0","method":"a","method":"b","id":1}`,
	`{"jsonrpc":"2.0","method":"a","METHOD":"c","id":1}`,
	`{"jsonrpc":"2.0","method":null,"id":1}`,
	`{"jsonrpc":2,"method":"m","id":1}`,
	`{"jsonrpc":"2.0","method":"mA","id":1}`,
	`{"jsonrpc":"2.0","method":"\xff","id":1}`,
	`{"jsonrpc":"2.0","method":"m","params":["a\"b"],"id":1}`,
	`{"jsonrpc":"2.0","method":"m","params":["\x"],"id":1}`,
	`{"jsonrpc":"2.0","method":"m","extra":{"nested":[1,{"a":"}"}]},"id":1}`,
	`{"jsonrpc":"2.0","method":"m","ídx":1,"id":1}`,
	`[]`, `[null]`, `[1]`, `"x"`, `null`, `{}`, `{"method":"a"} x`, ``, `   `, `{`, `[{"method":"a"},]`,
	`[{"jsonrpc":"2.0","method":"a","id":1},{"jsonrpc":"2.0","method":"b","params":[],"id":2}]`,
	`[{"jsonrpc":"2.0","method":"a","id":1},null]`,
}

func requireSameRequests(t *testing.T, body []byte) {
	t.Helper()
	_, decoded, _, decodeErr := parseRequestDecoded(bytes.Clone(body))
	borrowed, ok := parseRequestBorrowed(body)
	if !ok {
		return
	}
	if decodeErr != nil && hasOutOfRangeNumber(body) {
		// Sonic's optdec decoder (the arm64 default) rejects float-overflowing
		// number literals anywhere in the body, the amd64 JIT decoder does not.
		// The borrowed path forwards them, as the JIT path always has.
		return
	}
	require.NoError(t, decodeErr, "borrowed path accepted a body the decoder rejects: %q", body)
	require.Equal(t, len(decoded), len(borrowed), "%q", body)
	for i := range decoded {
		want, got := decoded[i], borrowed[i]
		require.NotNil(t, got)
		require.Equal(t, want.Jsonrpc, got.Jsonrpc, "%q", body)
		require.Equal(t, want.Method, got.Method, "%q", body)
		require.Equal(t, want.Params == nil, got.Params == nil, "params presence %q", body)
		require.Equal(t, string(want.Params), string(got.Params), "%q", body)
		require.Equal(t, want.ID == nil, got.ID == nil, "id presence %q", body)
		require.Equal(t, string(want.ID), string(got.ID), "%q", body)
	}
}

func TestParseRequestBorrowedMatchesDecoder(t *testing.T) {
	for _, seed := range parseRequestSeeds {
		requireSameRequests(t, []byte(seed))
	}
	for _, body := range benchfixture.Requests() {
		requireSameRequests(t, body)
	}
}

func TestParseRequestBorrowedCoverage(t *testing.T) {
	for _, body := range benchfixture.Requests() {
		_, ok := parseRequestBorrowed(body)
		require.True(t, ok, "common request shapes must take the borrowed path: %q", body)
	}
	for _, body := range []string{
		`{"jsonrpc":"2.0","method":"m","params":["a\"b"],"id":1}`,
		`{"jsonrpc":"2.0","Method":"m","id":1}`,
		`{"jsonrpc":"2.0","method":null,"id":1}`,
		`[{"jsonrpc":"2.0","method":"a","id":1},null]`,
	} {
		_, ok := parseRequestBorrowed([]byte(body))
		require.False(t, ok, "must fall back to the decoder: %q", body)
	}
}

func TestParseRequestBorrowsAndOwns(t *testing.T) {
	var req protocol.Request
	req.SetBody([]byte(`{"jsonrpc":"2.0","method":"m","params":[1],"id":1}`))
	_, reqs, _, err := ParseRequest(&req)
	require.NoError(t, err)
	require.Len(t, reqs, 1)
	// Params keeps len == cap so an append can never write into the body.
	require.Equal(t, len(reqs[0].Params), cap(reqs[0].Params))

	OwnRequests(reqs)
	req.SetBody([]byte(strings.Repeat("x", 51)))
	require.Equal(t, RPCMethod("m"), reqs[0].Method)
	require.Equal(t, "2.0", reqs[0].Jsonrpc)
	require.Equal(t, "[1]", string(reqs[0].Params))
	require.Equal(t, "1", string(reqs[0].ID))
}

func FuzzParseRequestBorrowedMatchesDecoder(f *testing.F) {
	for _, seed := range parseRequestSeeds {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		requireSameRequests(t, body)
	})
}

// hasOutOfRangeNumber reports whether any number token outside strings
// overflows float64.
func hasOutOfRangeNumber(body []byte) bool {
	inString := false
	for i := 0; i < len(body); i++ {
		c := body[i]
		if inString {
			if c == '\\' {
				i++
			} else if c == '"' {
				inString = false
			}
			continue
		}
		if c == '"' {
			inString = true
			continue
		}
		if c == '-' || (c >= '0' && c <= '9') {
			j := i
			for j < len(body) && strings.IndexByte("0123456789+-.eE", body[j]) >= 0 {
				j++
			}
			if _, err := strconv.ParseFloat(string(body[i:j]), 64); errors.Is(err, strconv.ErrRange) {
				return true
			}
			i = j - 1
		}
	}
	return false
}
