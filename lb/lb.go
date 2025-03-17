package lb

import (
	"context"
	"errors"
	"fmt"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"strconv"
	"strings"
	"time"

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
	"github.com/bytedance/sonic"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"go.opentelemetry.io/otel/trace"
	"net"
	"net/http"
)

type LoadBalancer struct {
	ctx                   context.Context
	nodeRefresherMap      map[string]*etcd.Discover
	Config                types.Config
	RpcMethodHandler      types.RPCMethodHandlerI
	Limiter               jsonrpc.Limiter
	HeightMap             jsonrpc.HeightMap
	nodeSelector          selector.Strategy
	nodeChannel           <-chan *discovery.TargetNode
	heightChannel         <-chan *discovery.ChainHeight
	rpcMethodTransportMap map[ejrpc.RPCMethod]*http.Transport
	preProcessors         types.PreProcessorProcessors
	preProcessorsHertz    types.PreProcessorProcessorsHertz
	postProcessors        types.PostProcessorProcessors
	postProcessorsHertz   types.PostProcessorProcessorsHertz
	defaultHttpTransport  *http.Transport
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

func NewLoadBalancer(ctx context.Context, nodeRefresherMap map[string]*etcd.Discover, config types.Config, rpcMethodHandler types.RPCMethodHandlerI, rpcMethodHandlerHertz types.RPCMethodHandlerIHertz, limiter jsonrpc.Limiter, heightMap jsonrpc.HeightMap, nodeChannel <-chan *discovery.TargetNode, heightChannel <-chan *discovery.ChainHeight) *LoadBalancer {
	var nodeSelector selector.Strategy
	switch config.NodeSelectStrategy {
	case "round_robin":
		nodeSelector = roundrobin.New(utils.PickNodes)
	case "random":
		nodeSelector = random.New(utils.PickNodes)
	}
	rpcMethodTransportMap := map[ejrpc.RPCMethod]*http.Transport{}
	for m, t := range config.RPCMethodTimeoutConfig {
		if t <= 0 {
			t = config.DefaultRPCTimeout
		}
		rpcMethodTransportMap[ejrpc.RPCMethod(m)] = NewHttpTransportWithTimeout(time.Duration(t)*time.Millisecond, config.ConnectionPoolSize)
	}
	return &LoadBalancer{
		ctx:                   ctx,
		nodeRefresherMap:      nodeRefresherMap,
		Config:                config,
		RpcMethodHandler:      rpcMethodHandler,
		Limiter:               limiter,
		HeightMap:             heightMap,
		nodeSelector:          nodeSelector,
		nodeChannel:           nodeChannel,
		heightChannel:         heightChannel,
		rpcMethodTransportMap: rpcMethodTransportMap,
		preProcessors:         jsonrpc.GetPreProcessor(&config, rpcMethodHandler, limiter),
		preProcessorsHertz:    jsonrpc.GetPreProcessorHertz(&config, rpcMethodHandlerHertz, limiter),
		postProcessors:        jsonrpc.GetPostProcessor(&config, rpcMethodHandler),
		postProcessorsHertz:   jsonrpc.GetPostProcessorHertz(&config, rpcMethodHandlerHertz),
		defaultHttpTransport:  NewHttpTransportWithTimeout(time.Duration(config.DefaultRPCTimeout)*time.Millisecond, config.ConnectionPoolSize),
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
			targetNode, err := lbnode.New(tempNode.NodeKey, tempNode.Address, tempNode.Port, types.DefaultLoadBalancerWeight)
			if err != nil {
				log.Error("failed to create node", err)
				continue
			}
			targetNode.SetState(tempNode.StateType)
			switch changeType {
			case 0:
				_ = lb.nodeSelector.UpsertNode(lb.ctx, chainId, role, targetNode)
			case 1:
				_ = lb.nodeSelector.RemoveNode(lb.ctx, chainId, role, targetNode)
			}

		case chainHeight := <-lb.heightChannel:
			lb.HeightMap.SetHeight(chainHeight.ChainId, chainHeight.LatestBlockNumber)
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

// func (lb *LoadBalancer) ServeHTTP(w http.ResponseWriter, r *http.Request, chainID string) {
func (lb *LoadBalancer) ServeHTTP(ctx context.Context, c *app.RequestContext, chainID string) {
	requestContext := lb.generateRequestContext(ctx, &c.Request)
	if requestContext.Error != nil {
		_, object, _ := ejrpc.BadRequest(requestContext.Error)
		c.JSON(consts.StatusOK, object)
		return
	}

	chainIDNum, err := parseNumber(chainID)
	if err != nil {
		_, object, _ := ejrpc.BadRequest(errors.New("invalid chain id"))
		c.JSON(consts.StatusOK, object)
		return
	}

	requestContext.ChainId = fmt.Sprint(chainIDNum)

	targetNode, err := lb.nodeSelector.GetNode(requestContext, "")
	if err != nil {
		log.Error("failed to get node, err ", err)
		_, object, _ := ejrpc.BadGateway(errors.New("no backends available"))
		c.JSON(consts.StatusOK, object)
		return
	}

	if targetNode.ReverseProxy == nil {
		log.Error("failed to get node, err ", errors.New("no reverse proxy available"))
		_, object, _ := ejrpc.BadGateway(errors.New("no reverse proxy available"))
		c.JSON(consts.StatusOK, object)
		return
	}
	ctx, roundTripSpan := jsonrpc.Tracer.Start(
		ctx,
		"RoundTrip")
	defer roundTripSpan.End()

	ctx, c, requestContext = lb.preProcessorsHertz.Call(ctx, c, requestContext)
	if len(c.Response.Body()) == 0 { // need to query upstream
		requestContext.UpstreamRelated = true
		_, upstreamSpan := jsonrpc.Tracer.Start(
			ctx,
			"Upstream",
			trace.WithAttributes(attribute.String("rpc_method", string(requestContext.Method))),
		)

		props := otel.GetTextMapPropagator()

		httpHeaders := http.Header{}
		c.Request.Header.VisitAll(func(key, value []byte) {
			// 为了确保后续使用不会受到影响，复制 key 和 value 的内容
			keyCopy := make([]byte, len(key))
			copy(keyCopy, key)
			valueCopy := make([]byte, len(value))
			copy(valueCopy, value)

			httpHeaders.Set(string(keyCopy), string(valueCopy))
		})

		props.Inject(ctx, propagation.HeaderCarrier(httpHeaders))
		targetNode.ReverseProxy.ServeHTTP(ctx, c)
		upstreamSpan.End()

		if c.Response.StatusCode() == consts.StatusGatewayTimeout {
			_, object, _ := ejrpc.GatewayTimeout(errors.New("reverse proxy gateway timeout"))
			c.JSON(consts.StatusGatewayTimeout, object)
			return
		}
		if c.Response.StatusCode() == consts.StatusBadGateway {
			_, object, _ := ejrpc.GatewayTimeout(errors.New("reverse proxy bad gateway"))
			c.JSON(consts.StatusBadGateway, object)
			return
		}
	}

	_, _, _ = lb.postProcessorsHertz.Call(ctx, c, requestContext)
}

func (lb *LoadBalancer) forwardDirector(host *lbnode.Node, inReq *protocol.Request) func(*http.Request) {

	return func(outReq *http.Request) {
		outReq.URL = cloneURL(outReq.URL)
		outReq.URL.Scheme = "http"
		outReq.URL.Host = host.Addr()
		outReq.URL.Path = inReq.URI().String()
		outReq.URL.RawPath = inReq.URI().String()
		outReq.URL.RawQuery = string(inReq.QueryString())
		outReq.RequestURI = ""
		outReq.Host = string(inReq.Host())
		if outReq.Host == "" {
			outReq.Host = string(inReq.URI().Host())
		}

		if _, ok := outReq.Header[headerUserAgent]; !ok {
			outReq.Header.Set(headerUserAgent, "")
		}
	}
}

func (lb *LoadBalancer) generateRequestContext(ctx context.Context, request *protocol.Request) *types.RequestContext {
	requestContext := lb.beforeProcess(ctx, request)

	requestContext.RawRequestBody, requestContext.RequestBody, requestContext.RequestBodySize, requestContext.Error = ejrpc.ParseRequest(request)
	if requestContext.Error != nil {
		return requestContext
	}
	requestContext.IsBatch = len(requestContext.RequestBody) > 1
	if !requestContext.IsBatch {
		requestContext.Method = requestContext.RequestBody[0].Method
	}
	for _, req := range requestContext.RequestBody {
		if requestContext.Error = ejrpc.ValidateRequest(req); requestContext.Error != nil {
			_, requestContext.ResponseBody, requestContext.Error = ejrpc.BadRequest(requestContext.Error)
			break
		}
	}

	requestContext.BlockContext = lb.ParseBlockContext(requestContext.RequestBody)
	return requestContext
}

func (lb *LoadBalancer) ParseBlockContext(requestBody []*ejrpc.RequestObject) *types.BlockContext {
	for _, value := range requestBody {
		var arr []sonic.NoCopyRawMessage
		err := sonic.Unmarshal(value.Params, &arr)
		if err != nil {
			log.Error("failed to unmarshal params", err)
			break
		}

		if len(arr) <= 0 {
			break
		}
		lastElem := arr[len(arr)-1]

		var ctx types.BlockContext
		if err := sonic.Unmarshal(lastElem, &ctx); err != nil {
			break
		}

		return &ctx
	}
	return nil
}

func (lb *LoadBalancer) parseBlockContext(requestBody []*ejrpc.RequestObject) *types.BlockContext {
	for _, value := range requestBody {
		var arr []interface{}
		err := sonic.Unmarshal(value.Params, &arr)
		if err != nil {
			log.Error("failed to unmarshal params", err)
			break
		}
		if len(arr) <= 0 {
			break
		}
		lastElem := arr[len(arr)-1]
		lastBytes, err := sonic.Marshal(lastElem)
		if err != nil {
			log.Error("failed to marshal params", err)
			break
		}
		var ctx types.BlockContext
		if err := sonic.Unmarshal(lastBytes, &ctx); err != nil {
			break
		}
		return &ctx
	}
	return nil
}

func (lb *LoadBalancer) beforeProcess(ctx context.Context, request *protocol.Request) *types.RequestContext {
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
		Host:            types.ProcessorHost(originHostFromContext(ctx, string(request.Host()))),
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

func NewHttpTransportWithTimeout(timeout time.Duration, connectionPoolSize int) *http.Transport {
	return &http.Transport{
		Proxy: nil,
		DialContext: (&net.Dialer{
			Timeout:   timeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          20 * connectionPoolSize,
		IdleConnTimeout:       30 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
		ResponseHeaderTimeout: timeout,
		MaxIdleConnsPerHost:   connectionPoolSize,
	}
}
