package lb

import (
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httputil"

	ejrpc "github.com/Chaintable/nodex-proxy/jsonrpc"
	"github.com/Chaintable/nodex-proxy/lb/jsonrpc"
	"github.com/Chaintable/nodex-proxy/lib/log"
	"github.com/Chaintable/nodex-proxy/node"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/Chaintable/nodex-proxy/utils"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"golang.org/x/exp/rand"
)

type LoadBalancer struct {
	nodeRefresherMap map[string]*node.Refresher
	BufferPool       httputil.BufferPool
	Config           types.Config
	RpcMethodHandler types.RPCMethodHandlerI
	Limiter          jsonrpc.Limiter
}

type BlockContext struct {
	BlockId *rpc.BlockNumberOrHash `json:"block_id"`
	Type    string                 `json:"type"`
}

var headerUserAgent = "User-Agent"

func NewLoadBalancer(nodeRefresherMap map[string]*node.Refresher, config types.Config, rpcMethodHandler types.RPCMethodHandlerI, limiter jsonrpc.Limiter) *LoadBalancer {
	return &LoadBalancer{
		nodeRefresherMap: nodeRefresherMap, BufferPool: utils.NewBufferPool(),
		Config:           config,
		RpcMethodHandler: rpcMethodHandler,
		Limiter:          limiter,
	}
}

func (lb *LoadBalancer) ServeHTTP(w http.ResponseWriter, r *http.Request, chainID string) {
	if lb.nodeRefresherMap[chainID] == nil {
		http.Error(w, "No backends available", http.StatusBadGateway)
		return
	}

	stateBackends, archiveBackends, blockHeight := lb.nodeRefresherMap[chainID].GetBackends()
	if len(stateBackends) == 0 && len(archiveBackends) == 0 {
		http.Error(w, "No backends available", http.StatusServiceUnavailable)
		return
	}

	reverseProxy := &httputil.ReverseProxy{
		Director:   lb.forwardDirector(stateBackends, archiveBackends, blockHeight, r),
		BufferPool: lb.BufferPool,
		Transport:  jsonrpc.NewTransport(lb.RpcMethodHandler, lb.Limiter, log.Logger(), &lb.Config),
	}

	reverseProxy.ServeHTTP(w, r)
}

func (lb *LoadBalancer) forwardDirector(stateBackends, archiveBackends []string, blockHeight *hexutil.Big, inReq *http.Request) func(*http.Request) {
	host := append(stateBackends, archiveBackends...)[rand.Intn(len(stateBackends)+len(archiveBackends))]
	if len(stateBackends) == 0 {
		stateBackends = archiveBackends
	}
	if len(archiveBackends) == 0 {
		archiveBackends = stateBackends
	}
	_, jsonObjects, _, err := ejrpc.ParseRequest(inReq)
	if err != nil {
		log.Error("failed to parse incoming request", err)
	}
	for _, value := range jsonObjects {
		var arr []interface{}
		err := json.Unmarshal(value.Params, &arr)
		if err != nil {
			log.Error("failed to unmarshal params", err)
			break
		}
		if len(arr) <= 0 {
			break
		}
		lastElem := arr[len(arr)-1]
		lastBytes, err := json.Marshal(lastElem)
		if err != nil {
			host = append(stateBackends, archiveBackends...)[rand.Intn(len(stateBackends)+len(archiveBackends))]
			log.Error("failed to marshal params", err)
			break
		}
		var ctx BlockContext
		if err := json.Unmarshal(lastBytes, &ctx); err != nil {
			host = append(stateBackends, archiveBackends...)[rand.Intn(len(stateBackends)+len(archiveBackends))]
			log.Error("failed to unmarshal params", err)
			break
		}
		if ctx.Type == "equal" && ctx.BlockId.BlockNumber != nil {
			stateBlockHeightLow := big.NewInt(0)
			stateBlockHeightLow.Sub(blockHeight.ToInt(), big.NewInt(64))
			if big.NewInt(ctx.BlockId.BlockNumber.Int64()).Cmp(stateBlockHeightLow) >= 0 {
				host = stateBackends[rand.Intn(len(stateBackends))]
			} else {
				host = archiveBackends[rand.Intn(len(archiveBackends))]
			}
		} else {
			host = archiveBackends[rand.Intn(len(archiveBackends))]
		}
	}

	return func(outReq *http.Request) {
		outReq.URL = cloneURL(outReq.URL)
		outReq.URL.Scheme = "http"
		outReq.URL.Host = host
		outReq.URL.Path = inReq.URL.Path
		outReq.URL.RawPath = inReq.URL.RawPath
		outReq.URL.RawQuery = inReq.URL.RawQuery
		outReq.RequestURI = ""
		outReq.Host = inReq.Host
		if outReq.Host == "" {
			outReq.Host = inReq.URL.Host
		}

		if _, ok := outReq.Header[headerUserAgent]; !ok {
			outReq.Header.Set(headerUserAgent, "")
		}
	}
}
