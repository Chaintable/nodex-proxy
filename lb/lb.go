package lb

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"

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
)

type LoadBalancer struct {
	ctx                   context.Context
	nodeRefresherMap      map[string]*etcd.Discover
	Config                types.Config
	Limiter               jsonrpc.Limiter
	HeightMap             jsonrpc.HeightMap
	GatewayStrategy       jsonrpc.GatewayStrategy
	MirrorMap             jsonrpc.MirrorMap
	MirrorLimiter         jsonrpc.MirrorLimiter
	NodeSelector          selector.Strategy
	nodeChannel           <-chan *discovery.TargetNode
	heightChannel         <-chan *discovery.ChainHeight
	gatewayChannel        <-chan *discovery.Gateway
	mirrorChannel         <-chan *discovery.MirrorTarget
	rpcMethodTransportMap map[ejrpc.RPCMethod]*http.Transport
	preProcessorsHertz    types.PreProcessorProcessorsHertz
	postProcessorsHertz   types.PostProcessorProcessorsHertz
	defaultHttpTransport  *http.Transport
	healthChecker         *NodeHealthChecker
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

func NewLoadBalancer(ctx context.Context, nodeRefresherMap map[string]*etcd.Discover, config types.Config,
	rpcMethodHandlerHertz types.RPCMethodHandlerIHertz, limiter jsonrpc.Limiter, heightMap jsonrpc.HeightMap,
	mirrorLimiter jsonrpc.MirrorLimiter,
	nodeChannel <-chan *discovery.TargetNode, heightChannel <-chan *discovery.ChainHeight,
	gatewayChannel <-chan *discovery.Gateway, mirrorChannel <-chan *discovery.MirrorTarget,
) *LoadBalancer {
	gatewayStrategy := jsonrpc.NewGatewayStrategy()
	mirrorMap := jsonrpc.NewMirrorMap()
	var nodeSelector selector.Strategy
	switch config.NodeSelectStrategy {
	case "round_robin":
		nodeSelector = roundrobin.New(utils.PickNodes)
	case "random":
		nodeSelector = random.New(utils.PickNodes, gatewayStrategy)
	}
	rpcMethodTransportMap := map[ejrpc.RPCMethod]*http.Transport{}
	for m, t := range config.RPCMethodTimeoutConfig {
		if t <= 0 {
			t = config.DefaultRPCTimeout
		}
		rpcMethodTransportMap[ejrpc.RPCMethod(m)] = NewHttpTransportWithTimeout(time.Duration(t)*time.Millisecond, config.ConnectionPoolSize)
	}
	// Create health checker with default timeout values
	healthCheckTimeout := 5 * time.Second
	if config.NodeHealthCheckTimeout > 0 {
		healthCheckTimeout = time.Duration(config.NodeHealthCheckTimeout) * time.Millisecond
	}
	maxWaitTime := 5 * time.Minute
	if config.NodeHealthCheckMaxWait > 0 {
		maxWaitTime = time.Duration(config.NodeHealthCheckMaxWait) * time.Millisecond
	}

	return &LoadBalancer{
		ctx:                   ctx,
		nodeRefresherMap:      nodeRefresherMap,
		Config:                config,
		Limiter:               limiter,
		HeightMap:             heightMap,
		GatewayStrategy:       gatewayStrategy,
		MirrorMap:             mirrorMap,
		MirrorLimiter:         mirrorLimiter,
		NodeSelector:          nodeSelector,
		nodeChannel:           nodeChannel,
		heightChannel:         heightChannel,
		gatewayChannel:        gatewayChannel,
		mirrorChannel:         mirrorChannel,
		rpcMethodTransportMap: rpcMethodTransportMap,
		preProcessorsHertz:    jsonrpc.GetPreProcessorHertz(&config, rpcMethodHandlerHertz, limiter, mirrorMap, mirrorLimiter),
		postProcessorsHertz:   jsonrpc.GetPostProcessorHertz(&config, rpcMethodHandlerHertz),
		defaultHttpTransport:  NewHttpTransportWithTimeout(time.Duration(config.DefaultRPCTimeout)*time.Millisecond, config.ConnectionPoolSize),
		healthChecker:         NewNodeHealthChecker(healthCheckTimeout, maxWaitTime),
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

			switch changeType {
			case etcd.EVENT_PUT:
				// Perform health check in background goroutine
				go func(node *discovery.TargetNode) {
					log.Info("performing health check for new node",
						log.Any("node_key", node.NodeKey),
						log.Any("address", fmt.Sprintf("%s:%d", node.Address, node.Port)),
						log.Any("chain_id", chainId))

					targetNode, err := lb.healthChecker.CheckNodeHealth(lb.ctx, node)
					if err != nil {
						log.Error("node health check failed, node will not be added",
							err,
							log.Any("node_key", node.NodeKey),
							log.Any("address", fmt.Sprintf("%s:%d", node.Address, node.Port)),
							log.Any("chain_id", chainId))
						return
					}

					log.Info("node health check passed, adding to pool",
						log.Any("node_key", node.NodeKey),
						log.Any("address", targetNode.Addr()),
						log.Any("chain_id", chainId))

					_ = lb.NodeSelector.UpsertNode(lb.ctx, chainId, role, targetNode)
				}(tempNode)

			case etcd.EVENT_DELETE:
				targetNode, err := lbnode.New(tempNode.NodeKey, tempNode.Address, tempNode.Port, types.DefaultWeight, role, lbnode.WithSource(tempNode.Source))
				if err != nil {
					log.Error("failed to create node", err)
					continue
				}
				targetNode.SetState(tempNode.StateType)
				_ = lb.NodeSelector.RemoveNode(lb.ctx, chainId, role, targetNode)
			}

		case chainHeight := <-lb.heightChannel:
			lb.HeightMap.SetHeight(chainHeight.ChainId, chainHeight.LatestBlockNumber)
			_ = lb.NodeSelector.UpdateChainHeight(lb.ctx, chainHeight.ChainId, chainHeight.LatestBlockNumber)

		case gateway := <-lb.gatewayChannel:
			chainId := gateway.ChainId

			switch gateway.ChangeType {
			case etcd.EVENT_PUT:
				lb.GatewayStrategy.UpdateGateway(chainId, *gateway)
			case etcd.EVENT_DELETE:
				lb.GatewayStrategy.DeleteGateway(chainId)
			}

		case mirror := <-lb.mirrorChannel:
			if mirror.Deleted {
				lb.MirrorMap.DeleteMirrorTarget(mirror.ChainId, mirror.AddrKey)
				lb.MirrorLimiter.RemoveLimit(mirror.ChainId, mirror.URL())
				log.Info("mirror target deleted",
					log.Any("chainId", mirror.ChainId),
					log.Any("addrKey", mirror.AddrKey))
			} else {
				lb.MirrorMap.AddMirrorTarget(mirror.ChainId, mirror.AddrKey, mirror)
				if mirror.RateLimit != nil && *mirror.RateLimit > 0 {
					lb.MirrorLimiter.UpdateLimit(mirror.ChainId, mirror.URL(), *mirror.RateLimit)
				}
				log.Info("mirror target added",
					log.Any("chainId", mirror.ChainId),
					log.Any("addrKey", mirror.AddrKey),
					log.Any("url", mirror.URL()))
			}
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

	ctx, roundTripSpan := jsonrpc.Tracer.Start(ctx, "RoundTrip")
	defer roundTripSpan.End()

	// Pre-processors and Post-processors
	ctx, c, requestContext = lb.preProcessorsHertz.Call(ctx, c, requestContext)
	defer func() {
		_, _, _ = lb.postProcessorsHertz.Call(ctx, c, requestContext)
	}()
	// If response is already set by pre-processors, return directly
	if len(c.Response.Body()) != 0 {
		return
	}

	_, upstreamSpan := jsonrpc.Tracer.Start(
		ctx,
		"Upstream",
		trace.WithAttributes(attribute.String("rpc_method", string(requestContext.Method))),
	)
	defer upstreamSpan.End()

	// Mark as upstream related request
	requestContext.UpstreamRelated = true

	targetNode, err := lb.NodeSelector.GetNode(requestContext, "")
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

	log.Debug("Selected target node", log.Any("node", targetNode.Addr()), log.Any("type", targetNode.NodeType), log.Any("chain_id", requestContext.ChainId))

	// If target node is archive, set archive flag
	if targetNode.NodeType == discovery.NodeTypeArchive {
		requestContext.Archive = true
	}

	// First attempt with state node
	lb.attemptRequest(ctx, c, targetNode)
	// Check if response contains error code -39006
	if lb.shouldRetryWithArchive(c, requestContext) {
		log.Info("Received error code -39006(StateBlockNotFound), retrying with archive node")

		// Reset response body for retry
		c.Response.Reset()

		// Retry with archive node
		requestContext.Archive = true

		targetNode, err := lb.NodeSelector.GetNode(requestContext, "")
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
		log.Debug("Selected archive target node", log.Any("node", targetNode.Addr()), log.Any("chain_id", requestContext.ChainId))
		lb.attemptRequest(ctx, c, targetNode)
	}
}

func (lb *LoadBalancer) attemptRequest(ctx context.Context, c *app.RequestContext, targetNode *lbnode.Node) {
	props := otel.GetTextMapPropagator()
	httpHeaders := http.Header{}
	c.Request.Header.VisitAll(func(key, value []byte) {
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		valueCopy := make([]byte, len(value))
		copy(valueCopy, value)
		httpHeaders.Set(string(keyCopy), string(valueCopy))
	})

	props.Inject(ctx, propagation.HeaderCarrier(httpHeaders))
	targetNode.ReverseProxy.ServeHTTP(ctx, c)

	if c.Response.StatusCode() == consts.StatusGatewayTimeout {
		_, object, _ := ejrpc.GatewayTimeout(errors.New("reverse proxy gateway timeout"))
		c.JSON(consts.StatusGatewayTimeout, object)
	}
	if c.Response.StatusCode() == consts.StatusBadGateway {
		_, object, _ := ejrpc.BadGateway(errors.New("reverse proxy bad gateway"))
		c.JSON(consts.StatusBadGateway, object)
	}
}

func (lb *LoadBalancer) shouldRetryWithArchive(c *app.RequestContext, requestContext *types.RequestContext) bool {
	// If already using archive node, do not retry
	if requestContext.Archive {
		return false
	}

	responseBody := c.Response.Body()
	if len(responseBody) == 0 {
		return false
	}

	log.Debug("Response received", log.Any("response", string(responseBody)))

	type rpcResponse struct {
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	var resp rpcResponse
	if err := sonic.Unmarshal(responseBody, &resp); err != nil {
		log.Error("failed to unmarshal response, err ", err)
		return false
	}
	if resp.Error != nil && resp.Error.Code == types.StateBlockNotFound {
		return true
	}
	return false
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
		log.Debug("ParseBlockContext", log.Any("params", arr), log.Any("method", value.Method))
		if len(arr) <= 0 {
			break
		}
		blockCtx := arr[len(arr)-1]
		if (value.Method == ejrpc.ContractMultiCall || value.Method == ejrpc.SimulateTransactions) && len(arr) > 1 {
			blockCtx = arr[1]
		}

		var ctx types.BlockContext
		if err := sonic.Unmarshal(blockCtx, &ctx); err != nil {
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

// GetMirrorMap returns the MirrorMap instance for external access
func (lb *LoadBalancer) GetMirrorMap() jsonrpc.MirrorMap {
	return lb.MirrorMap
}

func (lb *LoadBalancer) GetMirrorLimiter() jsonrpc.MirrorLimiter {
	return lb.MirrorLimiter
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
