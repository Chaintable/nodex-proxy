package lb

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"

	"github.com/Chaintable/nodex-proxy/discovery"
	"github.com/Chaintable/nodex-proxy/discovery/etcd"
	ejrpc "github.com/Chaintable/nodex-proxy/jsonrpc"
	"github.com/Chaintable/nodex-proxy/lb/jsonrpc"
	"github.com/Chaintable/nodex-proxy/lb/lbnode"
	"github.com/Chaintable/nodex-proxy/lb/selector"
	"github.com/Chaintable/nodex-proxy/lb/selector/random"
	"github.com/Chaintable/nodex-proxy/lb/selector/roundrobin"
	"github.com/Chaintable/nodex-proxy/lib/log"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/Chaintable/nodex-proxy/utils"
	"go.opentelemetry.io/otel/trace"
)

type LoadBalancer struct {
	ctx              context.Context
	nodeRefresherMap map[string]*etcd.Discover
	BufferPool       httputil.BufferPool
	Config           types.Config
	RpcMethodHandler types.RPCMethodHandlerI
	Limiter          jsonrpc.Limiter
	nodeSelector     selector.Strategy
	nodeChannel      <-chan *discovery.TargetNode
	heightChannel    <-chan *discovery.ChainHeight
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

func NewLoadBalancer(ctx context.Context, nodeRefresherMap map[string]*etcd.Discover, config types.Config, rpcMethodHandler types.RPCMethodHandlerI, limiter jsonrpc.Limiter, nodeChannel <-chan *discovery.TargetNode, heightChannel <-chan *discovery.ChainHeight) *LoadBalancer {
	var nodeSelector selector.Strategy
	switch config.NodeSelectStrategy {
	case "round_robin":
		nodeSelector = roundrobin.New(utils.PickNodes)
	case "random":
		nodeSelector = random.New(utils.PickNodes)
	}
	return &LoadBalancer{
		ctx:              ctx,
		nodeRefresherMap: nodeRefresherMap, BufferPool: utils.NewBufferPool(),
		Config:           config,
		RpcMethodHandler: rpcMethodHandler,
		Limiter:          limiter,
		nodeSelector:     nodeSelector,
		nodeChannel:      nodeChannel,
		heightChannel:    heightChannel,
	}
}

func (lb *LoadBalancer) BackgroundRefreshNode() {
	for {
		select {
		case <-lb.ctx.Done():
			return
		case tempNode := <-lb.nodeChannel:
			chainId := tempNode.ChainId
			role := tempNode.NodeType
			changeType := tempNode.ChangeType
			targetNode := lbnode.New(tempNode.NodeKey, tempNode.Address, tempNode.Port, types.DefaultLoadBalancerWeight)
			targetNode.SetState(tempNode.StateType)
			switch changeType {
			case 0:
				_ = lb.nodeSelector.UpsertNode(lb.ctx, chainId, role, targetNode)
			case 1:
				_ = lb.nodeSelector.RemoveNode(lb.ctx, chainId, role, targetNode)
			}

		case chainHeight := <-lb.heightChannel:
			_ = lb.nodeSelector.UpdateChainHeight(lb.ctx, chainHeight.ChainId, chainHeight.LatestBlockNumber)
		}
	}
}

func parseNumber(s string) (int64, error) {
	s = strings.TrimSpace(s)

	// 判断是否以 "0x" 或 "0X" 开头
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		// 截掉前缀，再以 16 进制解析
		return strconv.ParseInt(s[2:], 16, 64)
	}

	// 否则按 10 进制解析
	return strconv.ParseInt(s, 10, 64)
}

func (lb *LoadBalancer) ServeHTTP(w http.ResponseWriter, r *http.Request, chainID string) {
	requestContext := lb.generateRequestContext(r)
	if requestContext.Error != nil {
		_, object, _ := ejrpc.BadRequest(requestContext.Error)
		data, _ := json.Marshal(object)
		w.WriteHeader(200)
		w.Write(data)
		return
	}

	chainIDNum, err := parseNumber(chainID)
	if err != nil {
		_, object, _ := ejrpc.BadRequest(errors.New("invalid chain id"))
		data, _ := json.Marshal(object)
		w.WriteHeader(200)
		w.Write(data)
		return
	}

	requestContext.ChainId = fmt.Sprint(chainIDNum)

	targetNode, err := lb.nodeSelector.GetNode(requestContext, "")
	if err != nil {
		_, object, _ := ejrpc.BadRequest(errors.New("no backends available"))
		data, _ := json.Marshal(object)
		w.WriteHeader(200)
		w.Write(data)
		return
	}

	reverseProxy := &httputil.ReverseProxy{
		Director:   lb.forwardDirector(targetNode, r),
		BufferPool: lb.BufferPool,
		Transport:  jsonrpc.NewTransport(requestContext, lb.RpcMethodHandler, lb.Limiter, log.Logger(), &lb.Config),
	}

	reverseProxy.ServeHTTP(w, r)
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
