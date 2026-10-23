package lb

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	ejrpc "github.com/Chaintable/nodex-proxy/jsonrpc"
	"github.com/Chaintable/nodex-proxy/lib/log"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/bytedance/sonic"
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
