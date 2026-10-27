// Package benchfixture provides synthetic, reproducible RPC workloads for benchmarks.
// These contain no production request or response data.
package benchfixture

import "strings"

// Responses includes a large hex multicall result, a structured block result,
// single errors, and a mixed batch with an error after a large result.
func Responses() map[string][]byte {
	multicall := `{"jsonrpc":"2.0","id":1,"result":"0x` + strings.Repeat("0123456789abcdef", 16384) + `"}`
	entry := `{"hash":"0x` + strings.Repeat("ab", 32) + `","input":"0x` + strings.Repeat("12", 128) + `","value":"0x0"}`
	block := `{"jsonrpc":"2.0","id":1,"result":{"transactions":[` + strings.TrimSuffix(strings.Repeat(entry+",", 512), ",") + `]}}`
	err := `{"jsonrpc":"2.0","id":2,"error":{"code":-39006,"message":"state block not found"}}`
	return map[string][]byte{
		"small":            []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1234"}`),
		"multicall256KiB":  []byte(multicall),
		"block":            []byte(block),
		"error":            []byte(err),
		"capitalizedError": []byte(strings.Replace(err, `"error"`, `"Error"`, 1)),
		"batch":            []byte("[" + multicall + "," + err + "]"),
	}
}

func Requests() map[string][]byte {
	call := `{"to":"0x` + strings.Repeat("ab", 20) + `","data":"0x` + strings.Repeat("12", 128) + `"}`
	multicall := `{"jsonrpc":"2.0","method":"contractMultiCall","params":[[` + strings.TrimSuffix(strings.Repeat(call+",", 128), ",") + `],{"block_id":"0x1234","type":"Equals"}],"id":1}`
	// Production multicall requests average about 2.7KB.
	multicall8 := `{"jsonrpc":"2.0","method":"contractMultiCall","params":[[` + strings.TrimSuffix(strings.Repeat(call+",", 8), ",") + `],{"block_id":"0x` + strings.Repeat("ab", 32) + `","type":"Equals"}],"id":1}`
	small := `{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x1234","latest"],"id":1}`
	return map[string][]byte{
		"small":      []byte(small),
		"multicall":  []byte(multicall),
		"multicall8": []byte(multicall8),
		"batch":      []byte("[" + multicall + "," + small + "]"),
	}
}
