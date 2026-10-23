package lb

import (
	"strings"

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
		// The search borrows request bytes. Own only the selected small argument so
		// returned strings cannot retain or alias the mutable, potentially large body.
		if err := sonic.UnmarshalString(strings.Clone(raw), &ctx); err != nil {
			continue
		}
		return &ctx
	}
	return nil
}

func blockContextArgument(params []byte, second bool) (string, int, error) {
	if second && !log.DebugEnabled() {
		// Stop at params[1]; do not decode the call payload or inspect trailing args.
		node, err := sonic.GetWithOptions(params, ast.SearchOptions{}, 1)
		if err == nil {
			raw, err := node.Raw()
			return raw, 2, err
		}
	}
	// Generic methods select the last argument. Also count the entire array when
	// debug logs need params_count, or when the second argument is absent.
	root, err := sonic.GetWithOptions(params, ast.SearchOptions{})
	if err != nil {
		return "", 0, err
	}
	if root.TypeSafe() == ast.V_NULL {
		return "", 0, nil
	}
	iter, err := root.Values()
	if err != nil {
		return "", 0, err
	}
	var node ast.Node
	var raw string
	count := 0
	for iter.Next(&node) {
		count++
		if !second || count == 2 {
			raw, err = node.Raw()
			if err != nil {
				return "", 0, err
			}
		}
	}
	if err := root.Check(); err != nil {
		return "", 0, err
	}
	return raw, count, nil
}
