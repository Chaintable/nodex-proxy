package lb

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"

	ejrpc "github.com/Chaintable/nodex-proxy/jsonrpc"
	"github.com/Chaintable/nodex-proxy/lib/log"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/bytedance/sonic"
	"github.com/bytedance/sonic/ast"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/stretchr/testify/require"
)

func decodedContext(raw []byte) *types.BlockContext {
	var ctx types.BlockContext
	if sonic.Unmarshal(raw, &ctx) != nil {
		return nil
	}
	return &ctx
}

func TestBlockContextArgumentSelection(t *testing.T) {
	first := `["0x` + strings.Repeat("ab", 4096) + `"]`
	contexts := []string{`null`, `"latest"`, `[]`, `0`, `true`, `{}`,
		`{"block_id":"0x1234","type":"Equals"}`,
		`{"block_id":"latest","type":"Equals"}`,
		`{"block_id":"pending","type":"Equals"}`,
		`{"block_id":"0x` + strings.Repeat("ab", 32) + `","type":"Contains"}`,
		`{"block_id":null,"type":"Contains"}`,
		`{"block_id":"0x1234","type":"Equals","type":"Contains"}`,
		`{"block_id":"not-a-block","type":"Equals"}`,
	}
	for _, method := range []ejrpc.RPCMethod{ejrpc.ContractMultiCall, ejrpc.SimulateTransactions, ejrpc.EstimateGas, "eth_multiCall"} {
		for i, ctx := range contexts {
			t.Run(string(method)+"/"+strconv.Itoa(i), func(t *testing.T) {
				raw := []byte("[" + first + "," + ctx + "]")
				if method != "eth_multiCall" {
					raw = []byte("[" + first + "," + ctx + ",false]")
				}
				before := bytes.Clone(raw)
				require.Equal(t, decodedContext([]byte(ctx)), (&LoadBalancer{}).ParseBlockContext([]*ejrpc.RequestObject{{Method: method, Params: raw}}))
				require.Equal(t, before, raw)
			})
		}
	}
}

func TestBlockContextMissingAndUnusedArguments(t *testing.T) {
	parser := &LoadBalancer{}
	for _, raw := range []string{`[]`, `null`, `{}`, `"text"`, `[`} {
		require.Nil(t, parser.ParseBlockContext([]*ejrpc.RequestObject{{Method: ejrpc.ContractMultiCall, Params: []byte(raw)}}), raw)
	}
	blockID := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)
	for _, method := range []ejrpc.RPCMethod{ejrpc.ContractMultiCall, ejrpc.SimulateTransactions, ejrpc.EstimateGas} {
		require.Equal(t, &types.BlockContext{BlockId: &blockID, Type: "Contains"}, parser.ParseBlockContext([]*ejrpc.RequestObject{{Method: method, Params: []byte(`[null]`)}}))
	}
	ctx := `{"block_id":"0x1234","type":"Equals"}`
	// Routing deliberately does not validate unused numeric values or trailing
	// arguments. The unchanged request bytes go to the upstream for validation.
	for _, raw := range []string{`[1e10000,` + ctx + `]`, `[[],` + ctx + `,invalid]`, `[[],` + ctx + `] trailing`} {
		require.Equal(t, decodedContext([]byte(ctx)), parser.ParseBlockContext([]*ejrpc.RequestObject{{Method: ejrpc.ContractMultiCall, Params: []byte(raw)}}))
	}
	require.Equal(t, decodedContext([]byte(ctx)), parser.ParseBlockContext([]*ejrpc.RequestObject{{Method: "generic", Params: []byte(`[null,false,` + ctx + `]`)}}))
}

func TestBlockContextBatchAndOwnership(t *testing.T) {
	params := []byte(`[["0x` + strings.Repeat("ab", 4096) + `"],{"block_id":"0x1234","type":"Equals"}]`)
	reqs := []*ejrpc.RequestObject{{Method: ejrpc.ContractMultiCall, Params: []byte(`[null,"not-a-context"]`)}, {Method: ejrpc.ContractMultiCall, Params: params}}
	expected := decodedContext([]byte(`{"block_id":"0x1234","type":"Equals"}`))
	actual := (&LoadBalancer{}).ParseBlockContext(reqs)
	require.Equal(t, expected, actual)
	for i := range params {
		params[i] = 'x'
	}
	require.Equal(t, expected, actual, "returned fields must own their bytes")
}

func TestBlockContextDebugCount(t *testing.T) {
	log.InitLogger("debug")
	defer log.InitLogger("error")
	raw, count, err := blockContextArgument([]byte(`[null,{"type":"Equals"},false]`), true)
	require.NoError(t, err)
	require.Equal(t, 3, count)
	require.Equal(t, `{"type":"Equals"}`, raw)
}

func FuzzBlockContextSelection(f *testing.F) {
	for _, ctx := range []string{`null`, `{}`, `"latest"`, `{"block_id":"0x1234","type":"Equals"}`} {
		f.Add([]byte(`{"nested":[1,2,3]}`), []byte(ctx), false)
	}
	f.Fuzz(func(t *testing.T, first, selected []byte, generic bool) {
		if !json.Valid(first) || !json.Valid(selected) {
			return
		}
		method := ejrpc.ContractMultiCall
		if generic {
			method = "generic"
		}
		raw := []byte("[" + string(first) + "," + string(selected) + "]")
		require.Equal(t, decodedContext(selected), (&LoadBalancer{}).ParseBlockContext([]*ejrpc.RequestObject{{Method: method, Params: raw}}))
	})
}

// legacyBlockContextArgument is the previous sonic-based implementation, kept
// as the reference for the scanner.
func legacyBlockContextArgument(params []byte, second bool) (string, int, error) {
	if second && !log.DebugEnabled() {
		node, err := sonic.GetWithOptions(params, ast.SearchOptions{}, 1)
		if err == nil {
			raw, err := node.Raw()
			return raw, 2, err
		}
	}
	var args []sonic.NoCopyRawMessage
	if err := sonic.UnmarshalString(string(params), &args); err != nil {
		return "", 0, err
	}
	if len(args) == 0 {
		return "", 0, nil
	}
	arg := args[len(args)-1]
	if second && len(args) > 1 {
		arg = args[1]
	}
	return string(arg), len(args), nil
}

func requireSameArgument(t *testing.T, params []byte, second bool) {
	t.Helper()
	if !json.Valid(params) {
		// Params are always part of a validated request body.
		return
	}
	wantRaw, wantCount, wantErr := legacyBlockContextArgument(params, second)
	gotRaw, gotCount, gotErr := blockContextArgument(params, second)
	if wantErr != nil && hasOutOfRangeNumber(params) {
		return // sonic optdec rejects float-overflowing literals; see jsonrpc tests.
	}
	require.Equal(t, wantErr == nil, gotErr == nil, "%q second=%v: %v / %v", params, second, wantErr, gotErr)
	require.Equal(t, wantCount, gotCount, "%q second=%v", params, second)
	// sonic optdec keeps trailing whitespace on raw number elements; padding
	// around a value never changes how it decodes.
	require.Equal(t, strings.TrimSpace(wantRaw), gotRaw, "%q second=%v", params, second)
}

func requireSameBlockContext(t *testing.T, raw string) {
	t.Helper()
	if !json.Valid([]byte(raw)) {
		return
	}
	var want types.BlockContext
	wantErr := sonic.UnmarshalString(raw, &want)
	got, gotErr := decodeBlockContext(raw)
	if wantErr != nil && hasOutOfRangeNumber([]byte(raw)) {
		return
	}
	require.Equal(t, wantErr == nil, gotErr == nil, "%q: %v / %v", raw, wantErr, gotErr)
	if wantErr == nil {
		require.Equal(t, &want, got, "%q", raw)
	}
}

var blockContextSeeds = []string{
	`{"block_id":"0x1234","type":"Equals"}`,
	`{"block_id":"0x` + strings.Repeat("ab", 32) + `","type":"Equals"}`,
	`{"block_id":"0x` + strings.Repeat("AB", 32) + `","type":"Contains"}`,
	`{"block_id":"0x` + strings.Repeat("zz", 32) + `","type":"Equals"}`,
	`{"block_id":"latest","type":"Equals"}`,
	`{"block_id":"earliest"}`, `{"block_id":"safe"}`, `{"block_id":"finalized"}`, `{"block_id":"pending"}`,
	`{"block_id":"0x8000000000000000","type":"Equals"}`,
	`{"block_id":"0x01","type":"Equals"}`, `{"block_id":"","type":"Equals"}`, `{"block_id":"12","type":"Equals"}`,
	`{"block_id":{"blockHash":"0x` + strings.Repeat("ab", 32) + `","requireCanonical":true},"type":"Equals"}`,
	`{"block_id":{"blockNumber":"0x10"},"type":"Equals"}`,
	`{"block_id":null,"type":"Contains"}`, `{"block_id":12,"type":"Equals"}`,
	`{"Block_ID":"0x1234","TYPE":"Equals"}`, `{"type":"Equals","type":"Contains"}`,
	`{"type":"Equals"}`, `{"type":"Équals"}`, `{"other":[1,{"a":"}"}],"type":"Equals"}`,
	`{}`, `null`, `"latest"`, `[]`, `0`, `true`,
}

func TestDecodeBlockContextMatchesSonic(t *testing.T) {
	for _, raw := range blockContextSeeds {
		requireSameBlockContext(t, raw)
	}
}

func TestBlockContextArgumentMatchesLegacy(t *testing.T) {
	ctx := `{"block_id":"0x1234","type":"Equals"}`
	for _, params := range []string{
		`[]`, ` [ ] `, `null`, ` null `, `[null]`, `[1]`, `[1,` + ctx + `]`, `[1,` + ctx + `,false]`,
		`[[{"to":"0x01","data":"0x02"}],` + ctx + `]`, `["a\"]b",` + ctx + `]`, `[ 1 , ` + ctx + ` ]`,
		`{}`, `"text"`, `0`,
		// Above nativeSkipThreshold, exercising the sonic skip path.
		`[["0x` + strings.Repeat("ab", 8<<10) + `"],` + ctx + `]`,
		`[["0x` + strings.Repeat("ab", 8<<10) + `"]]`,
		`[["0x` + strings.Repeat("ab", 8<<10) + `"],` + ctx + `,false] trailing`,
	} {
		for _, second := range []bool{false, true} {
			requireSameArgument(t, []byte(params), second)
		}
	}
}

func FuzzBlockContextArgumentMatchesLegacy(f *testing.F) {
	f.Add([]byte(`[[1,2],{"block_id":"0x1234","type":"Equals"},false]`), true)
	f.Add([]byte(`["a\"]b",null]`), false)
	f.Fuzz(func(t *testing.T, params []byte, second bool) {
		requireSameArgument(t, params, second)
	})
}

func FuzzDecodeBlockContextMatchesSonic(f *testing.F) {
	for _, seed := range blockContextSeeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		requireSameBlockContext(t, raw)
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
