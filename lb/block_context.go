package lb

import (
	"strings"
	"unsafe"

	ejrpc "github.com/Chaintable/nodex-proxy/jsonrpc"
	"github.com/Chaintable/nodex-proxy/lib/log"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/bytedance/sonic"
	"github.com/bytedance/sonic/ast"
	"github.com/ethereum/go-ethereum/rpc"
)

// ParseBlockContext reads only the argument needed for routing. Request bytes
// are forwarded unchanged; validating other arguments belongs to the upstream.
func (lb *LoadBalancer) ParseBlockContext(requestBody []*ejrpc.RequestObject) *types.BlockContext {
	for _, req := range requestBody {
		second := req.Method == ejrpc.ContractMultiCall || req.Method == ejrpc.SimulateTransactions || req.Method == ejrpc.EstimateGas
		raw, count, err := blockContextArgument(req.Params, second)
		if err != nil {
			log.Error("failed to unmarshal params", err)
			continue
		}
		if log.DebugEnabled() {
			log.Debug("ParseBlockContext", log.Any("method", req.Method), log.Any("params_count", count))
		}
		if count == 0 {
			continue
		}
		if second && count == 1 {
			blockID := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)
			return &types.BlockContext{BlockId: &blockID, Type: "Contains"}
		}
		var ctx types.BlockContext
		// raw borrows request bytes. Own only the selected small argument so
		// returned strings cannot retain or alias the mutable, potentially large body.
		if err := sonic.UnmarshalString(strings.Clone(raw), &ctx); err != nil {
			continue
		}
		return &ctx
	}
	return nil
}

// blockContextArgument returns the routing argument, borrowed from params, and
// the argument count. Second-argument methods select params[1]; others the last.
func blockContextArgument(params []byte, second bool) (string, int, error) {
	if second && !log.DebugEnabled() {
		// Stop at params[1]: the AST search skips a large call list faster than a
		// split and never looks at trailing arguments.
		node, err := sonic.GetWithOptions(params, ast.SearchOptions{}, 1)
		if err == nil {
			raw, err := node.Raw()
			return raw, 2, err
		}
	}
	// Split without copying: sonic.Unmarshal([]byte) copies the whole input
	// first, and building an AST costs more for typical small params. This also
	// counts every argument when debug logs need params_count.
	var args []sonic.NoCopyRawMessage
	if err := sonic.UnmarshalString(borrowString(params), &args); err != nil {
		return "", 0, err
	}
	if len(args) == 0 {
		return "", 0, nil
	}
	arg := args[len(args)-1]
	if second && len(args) > 1 {
		arg = args[1]
	}
	return borrowString(arg), len(args), nil
}

// borrowString views b as a string without copying. The caller must not let
// the result outlive b or observe it while b changes.
func borrowString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}
