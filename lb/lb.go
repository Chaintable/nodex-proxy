package lb

import (
	"context"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httputil"
	"time"

	ejrpc "github.com/Chaintable/nodex-proxy/jsonrpc"
	"github.com/Chaintable/nodex-proxy/lb/jsonrpc"
	"github.com/Chaintable/nodex-proxy/lb/lbnode"
	"github.com/Chaintable/nodex-proxy/lb/selector"
	"github.com/Chaintable/nodex-proxy/lb/selector/random"
	"github.com/Chaintable/nodex-proxy/lb/selector/roundrobin"
	"github.com/Chaintable/nodex-proxy/lib/log"
	"github.com/Chaintable/nodex-proxy/node"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/Chaintable/nodex-proxy/utils"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"go.opentelemetry.io/otel/trace"
)

type LoadBalancer struct {
	nodeRefresherMap map[string]*node.Refresher
	BufferPool       httputil.BufferPool
	Config           types.Config
	RpcMethodHandler types.RPCMethodHandlerI
	Limiter          jsonrpc.Limiter
	nodeSelector     selector.Strategy
}

var headerUserAgent = "User-Agent"

type jrpcxContextKeyType int

const (
	originHostKey jrpcxContextKeyType = iota

	DBKBiz           = "x-dbk-biz"
	DBKSourceHost    = "x-dbk-source-host"
	DBKSource        = "x-dbk-source"
	DBKEnv           = "x-dbk-env"
	DBKServerVersion = "x-dbk-server-version"
)

func NewLoadBalancer(nodeRefresherMap map[string]*node.Refresher, config types.Config, rpcMethodHandler types.RPCMethodHandlerI, limiter jsonrpc.Limiter) *LoadBalancer {
	var nodeSelector selector.Strategy
	switch config.NodeSelectStrategy {
	case "round_robin":
		nodeSelector = roundrobin.New()
	case "random":
		nodeSelector = random.New()
	}
	return &LoadBalancer{
		nodeRefresherMap: nodeRefresherMap, BufferPool: utils.NewBufferPool(),
		Config:           config,
		RpcMethodHandler: rpcMethodHandler,
		Limiter:          limiter,
		nodeSelector:     nodeSelector,
	}
}

func (lb *LoadBalancer) ServeHTTP(w http.ResponseWriter, r *http.Request, chainID string) {
	if lb.nodeRefresherMap[chainID] == nil {
		http.Error(w, "No backends available", http.StatusBadGateway)
		return
	}
	requestContext := lb.generateRequestContext(r)

	stateBackends, archiveBackends, blockHeight := lb.nodeRefresherMap[chainID].GetBackends()
	if len(stateBackends) == 0 && len(archiveBackends) == 0 {
		http.Error(w, "No backends available", http.StatusServiceUnavailable)
		return
	}
	targetNode, err := lb.getNode(requestContext, stateBackends, archiveBackends, blockHeight)
	if err != nil {
		http.Error(w, "No backends available", http.StatusServiceUnavailable)
		return
	}

	reverseProxy := &httputil.ReverseProxy{
		Director:   lb.forwardDirector(targetNode, r),
		BufferPool: lb.BufferPool,
		Transport:  jsonrpc.NewTransport(requestContext, lb.RpcMethodHandler, lb.Limiter, log.Logger(), &lb.Config),
	}

	reverseProxy.ServeHTTP(w, r)
}

func (lb *LoadBalancer) getNode(requestContext *types.RequestContext, stateBackends, archiveBackends []*lbnode.Node, blockHeight *hexutil.Big) (*lbnode.Node, error) {
	var backupNodes []*lbnode.Node
	backupNodes = append(backupNodes, stateBackends...)
	backupNodes = append(backupNodes, archiveBackends...)

	targetNode, err := lb.nodeSelector.GetNode(nil, backupNodes, "")
	if err != nil {
		return nil, err
	}

	if len(stateBackends) == 0 {
		stateBackends = archiveBackends
	}
	if len(archiveBackends) == 0 {
		archiveBackends = stateBackends
	}
	if requestContext.BlockContext != nil {
		if requestContext.BlockContext.Type == "equal" && requestContext.BlockContext.BlockId.BlockNumber != nil {
			stateBlockHeightLow := big.NewInt(0)
			stateBlockHeightLow.Sub(blockHeight.ToInt(), big.NewInt(64))
			if big.NewInt(requestContext.BlockContext.BlockId.BlockNumber.Int64()).Cmp(stateBlockHeightLow) >= 0 {
				targetNode, err = lb.nodeSelector.GetNode(nil, stateBackends, "")
				if err != nil {
					return nil, err
				}
			} else {
				targetNode, err = lb.nodeSelector.GetNode(nil, archiveBackends, "")
				if err != nil {
					return nil, err
				}
			}
		} else {
			targetNode, err = lb.nodeSelector.GetNode(nil, stateBackends, "")
			if err != nil {
				return nil, err
			}
		}
	}
	return targetNode, nil
}

func (lb *LoadBalancer) forwardDirector(host *lbnode.Node, inReq *http.Request) func(*http.Request) {

	return func(outReq *http.Request) {
		outReq.URL = cloneURL(outReq.URL)
		outReq.URL.Scheme = "http"
		outReq.URL.Host = host.Addr()
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

func (lb *LoadBalancer) generateRequestContext(request *http.Request) *types.RequestContext {
	requestContext := lb.beforeProcess(request)

	requestContext.RawRequestBody, requestContext.RequestBody, requestContext.RequestBodySize, requestContext.Error = ejrpc.ParseRequest(request)
	if requestContext.Error != nil {
		return requestContext
	}
	requestContext.BlockContext = lb.parseBlockContext(requestContext.RequestBody)
	return requestContext
}

func (lb *LoadBalancer) parseBlockContext(requestBody []*ejrpc.RequestObject) *types.BlockContext {
	for _, value := range requestBody {
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
			log.Error("failed to marshal params", err)
			break
		}
		var ctx types.BlockContext
		if err := json.Unmarshal(lastBytes, &ctx); err != nil {
			log.Error("failed to unmarshal params", err)
			break
		}
		return &ctx
	}
	return nil
}

func (lb *LoadBalancer) beforeProcess(request *http.Request) *types.RequestContext {
	ctx := request.Context()
	span := trace.SpanFromContext(ctx)
	traceID := span.SpanContext().TraceID().String()

	sourceBiz := request.Header.Get(DBKBiz)
	sourceHost := request.Header.Get(DBKSourceHost)
	sourceIP := request.Header.Get(DBKSource)
	sourceEnv := request.Header.Get(DBKEnv)
	sourceServerVersion := request.Header.Get(DBKServerVersion)

	// TODO: add some general metrics
	return &types.RequestContext{
		Context:             ctx,
		RequestId:           traceID,
		SourceBiz:           sourceBiz,
		SourceHost:          sourceHost,
		SourceIP:            sourceIP,
		SourceEnv:           sourceEnv,
		SourceServerVersion: sourceServerVersion,

		Start:           time.Now(),
		Method:          "unknown",
		Host:            types.ProcessorHost(originHostFromContext(ctx, request.Host)),
		Target:          "native",
		UpstreamRelated: false,
	}
}

func originHostFromContext(ctx context.Context, defaultHost string) string {
	if ctx == nil {
		return defaultHost
	}
	if originHost, ok := ctx.Value(originHostKey).(string); ok {
		return originHost
	}
	return defaultHost
}
