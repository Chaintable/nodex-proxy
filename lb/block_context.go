package lb

import (
	"strings"
	"unsafe"

	"github.com/Chaintable/nodex-proxy/internal/jsonscan"
	ejrpc "github.com/Chaintable/nodex-proxy/jsonrpc"
	"github.com/Chaintable/nodex-proxy/lib/log"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/bytedance/sonic"
	"github.com/bytedance/sonic/ast"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
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
		ctx, err := decodeBlockContext(raw)
		if err != nil {
			continue
		}
		return ctx
	}
	return nil
}

type paramsError string

func (e paramsError) Error() string { return string(e) }

const errMalformedParams = paramsError("malformed params")

// nativeSkipThreshold is the params size above which locating params[1] goes
// through sonic's SIMD skipper instead of the Go scanner.
const nativeSkipThreshold = 8 << 10

// blockContextArgument returns the routing argument, borrowed from params, and
// the argument count. Second-argument methods select params[1]; others the last.
//
// Params come from a request body that was already validated as JSON, so the
// walk only finds element boundaries. Second-argument methods stop at
// params[1] and report a count of 2 without looking at trailing arguments,
// unless debug logging needs the full count.
func blockContextArgument(params []byte, second bool) (string, int, error) {
	s := borrowString(params)
	i := jsonscan.SkipSpace(s, 0)
	if i >= len(s) {
		return "", 0, errMalformedParams
	}
	if s[i] != '[' {
		// A null params list means no arguments, as when decoding into a slice.
		if jsonscan.SkipSpace(s, i+4) == len(s) && strings.HasPrefix(s[i:], "null") {
			return "", 0, nil
		}
		return "", 0, errMalformedParams
	}

	stopAtSecond := second && !log.DebugEnabled()
	if stopAtSecond && len(params) > nativeSkipThreshold {
		// Sonic's native skip outruns the scanner across a large first argument.
		if node, err := sonic.GetWithOptions(params, ast.SearchOptions{}, 1); err == nil {
			raw, err := node.Raw()
			return raw, 2, err
		}
	}
	var (
		count            int
		lastS, lastE     int
		secondS, secondE int
	)
	end, ok := jsonscan.ScanArray(s, i, func(idx, vs, ve int) bool {
		count++
		lastS, lastE = vs, ve
		if idx == 1 {
			secondS, secondE = vs, ve
			return !stopAtSecond
		}
		return true
	})
	if !ok {
		return "", 0, errMalformedParams
	}
	if count == 0 {
		return "", 0, nil
	}
	if !(stopAtSecond && count == 2) && jsonscan.SkipSpace(s, end) != len(s) {
		// Only a complete walk can tell trailing bytes apart.
		return "", 0, errMalformedParams
	}
	if second && count > 1 {
		return s[secondS:secondE], count, nil
	}
	return s[lastS:lastE], count, nil
}

// decodeBlockContext decodes a routing argument into an owned BlockContext.
// The usual {"block_id":"<string>","type":"<string>"} shape is decoded
// directly, using go-ethereum's own string parsing for block_id; every other
// shape goes through sonic so the result never differs from it.
func decodeBlockContext(raw string) (*types.BlockContext, error) {
	if ctx, err, handled := decodeBlockContextFast(raw); handled {
		return ctx, err
	}
	var ctx types.BlockContext
	// raw borrows request bytes. Own only the selected small argument so
	// returned strings cannot retain or alias the mutable, potentially large body.
	if err := sonic.UnmarshalString(strings.Clone(raw), &ctx); err != nil {
		return nil, err
	}
	return &ctx, nil
}

func decodeBlockContextFast(raw string) (ctx *types.BlockContext, err error, handled bool) {
	if len(raw) == 0 || raw[0] != '{' {
		return nil, nil, false
	}
	var (
		blockID, typ       string
		hasBlockID, hasTyp bool
		declined           bool
	)
	_, ok := jsonscan.ScanObject(raw, 0, func(key string, keyEscaped bool, vs, ve int) bool {
		if keyEscaped || !jsonscan.IsPlainASCII(key) {
			declined = true
			return false
		}
		var dst *string
		var seen *bool
		switch key {
		case "block_id":
			dst, seen = &blockID, &hasBlockID
		case "type":
			dst, seen = &typ, &hasTyp
		default:
			// sonic matches field names case-insensitively.
			if strings.EqualFold(key, "block_id") || strings.EqualFold(key, "type") {
				declined = true
				return false
			}
			return true
		}
		value := raw[vs:ve]
		if *seen || value[0] != '"' {
			declined = true
			return false
		}
		content := value[1 : len(value)-1]
		if strings.IndexByte(content, '\\') >= 0 || !jsonscan.IsPlainASCII(content) {
			declined = true
			return false
		}
		*dst, *seen = content, true
		return true
	})
	if !ok || declined {
		return nil, nil, false
	}

	ctx = &types.BlockContext{Type: ownBlockContextType(typ)}
	if hasBlockID {
		bnh, err := parseBlockIDString(blockID)
		if err != nil {
			return nil, err, true
		}
		ctx.BlockId = bnh
	}
	return ctx, nil, true
}

// parseBlockIDString mirrors the string branch of
// rpc.BlockNumberOrHash.UnmarshalJSON for a plain (escape-free) JSON string.
func parseBlockIDString(input string) (*rpc.BlockNumberOrHash, error) {
	var bn rpc.BlockNumber
	switch input {
	case "earliest":
		bn = rpc.EarliestBlockNumber
	case "latest":
		bn = rpc.LatestBlockNumber
	case "pending":
		bn = rpc.PendingBlockNumber
	case "finalized":
		bn = rpc.FinalizedBlockNumber
	case "safe":
		bn = rpc.SafeBlockNumber
	default:
		if len(input) == 66 {
			var hash common.Hash
			if err := hash.UnmarshalText([]byte(input)); err != nil {
				return nil, err
			}
			return &rpc.BlockNumberOrHash{BlockHash: &hash}, nil
		}
		n, err := hexutil.DecodeUint64(input)
		if err != nil {
			return nil, err
		}
		if n > uint64(1<<63-1) {
			return nil, paramsError("blocknumber too high")
		}
		bn = rpc.BlockNumber(n)
	}
	return &rpc.BlockNumberOrHash{BlockNumber: &bn}, nil
}

// ownBlockContextType returns typ without retaining the request bytes.
func ownBlockContextType(typ string) string {
	switch typ {
	case "":
		return ""
	case "Equals":
		return "Equals"
	case "Contains":
		return "Contains"
	default:
		return strings.Clone(typ)
	}
}

// borrowString views b as a string without copying. The caller must not let
// the result outlive b or observe it while b changes.
func borrowString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}
