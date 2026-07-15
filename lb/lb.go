package lb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
	hzclient "github.com/cloudwego/hertz/pkg/app/client"
	hzconfig "github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/ethereum/go-ethereum/rpc"
	"go.opentelemetry.io/otel/trace"
)

type LoadBalancer struct {
	ctx                 context.Context
	nodeRefresherMap    map[string]*etcd.Discover
	Config              types.Config
	Limiter             jsonrpc.Limiter
	HeightMap           jsonrpc.HeightMap
	GatewayStrategy     jsonrpc.GatewayStrategy
	MirrorMap           jsonrpc.MirrorMap
	MirrorLimiter       jsonrpc.MirrorLimiter
	NodeSelector        selector.Strategy
	nodeChannel         <-chan *discovery.TargetNode
	heightChannel       <-chan *discovery.ChainHeight
	gatewayChannel      <-chan *discovery.Gateway
	mirrorChannel       <-chan *discovery.MirrorTarget
	versionChannel      <-chan *discovery.ChainVersion
	preProcessorsHertz  types.PreProcessorProcessorsHertz
	postProcessorsHertz types.PostProcessorProcessorsHertz
	healthChecker       *NodeHealthChecker
	chainVersionRouter  *ChainVersionRouter
	nodeGens            *nodeGenerations
}

type jrpcxContextKeyType int

const (
	originHostKey jrpcxContextKeyType = iota

	DBKBiz           = "x-dbk-biz"
	DBKSourceHost    = "x-dbk-source-host"
	DBKSource        = "x-dbk-source"
	DBKEnv           = "x-dbk-env"
	DBKServerVersion = "x-dbk-server-version"
	NodexNodeType    = "x-nodex-node-type"
)

func NewLoadBalancer(ctx context.Context, nodeRefresherMap map[string]*etcd.Discover, config types.Config,
	rpcMethodHandlerHertz types.RPCMethodHandlerIHertz, limiter jsonrpc.Limiter, heightMap jsonrpc.HeightMap,
	mirrorLimiter jsonrpc.MirrorLimiter,
	nodeChannel <-chan *discovery.TargetNode, heightChannel <-chan *discovery.ChainHeight,
	gatewayChannel <-chan *discovery.Gateway, mirrorChannel <-chan *discovery.MirrorTarget,
	versionChannel <-chan *discovery.ChainVersion,
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
	// All node reverse proxies share one upstream client; its per-host pools
	// survive node upserts so etcd refreshes no longer trigger reconnect storms.
	if err := lbnode.InitSharedClient(
		hzclient.WithMaxConnsPerHost(config.ConnectionPoolSize),
		hzclient.WithMaxIdleConnDuration(time.Duration(config.ConnMaxIdleDuration)*time.Millisecond),
		hzclient.WithMaxConnWaitTimeout(time.Duration(config.ConnMaxWaitTimeout)*time.Millisecond),
		hzclient.WithDialTimeout(time.Duration(config.ConnDialTimeout)*time.Millisecond),
		hzclient.WithClientReadTimeout(time.Duration(config.DefaultRPCTimeout)*time.Millisecond),
		hzclient.WithKeepAlive(true),
	); err != nil {
		log.Error("failed to init shared upstream client, falling back to defaults", err)
	}
	// Create health checker with default timeout values
	healthCheckTimeout := 5 * time.Second
	if config.NodeHealthCheckTimeout > 0 {
		healthCheckTimeout = time.Duration(config.NodeHealthCheckTimeout) * time.Second
	}
	maxWaitTime := 5 * time.Minute
	if config.NodeHealthCheckMaxWait > 0 {
		maxWaitTime = time.Duration(config.NodeHealthCheckMaxWait) * time.Second
	}

	return &LoadBalancer{
		ctx:                 ctx,
		nodeRefresherMap:    nodeRefresherMap,
		Config:              config,
		Limiter:             limiter,
		HeightMap:           heightMap,
		GatewayStrategy:     gatewayStrategy,
		MirrorMap:           mirrorMap,
		MirrorLimiter:       mirrorLimiter,
		NodeSelector:        nodeSelector,
		nodeChannel:         nodeChannel,
		heightChannel:       heightChannel,
		gatewayChannel:      gatewayChannel,
		mirrorChannel:       mirrorChannel,
		versionChannel:      versionChannel,
		preProcessorsHertz:  jsonrpc.GetPreProcessorHertz(&config, rpcMethodHandlerHertz, limiter, mirrorMap, mirrorLimiter),
		postProcessorsHertz: jsonrpc.GetPostProcessorHertz(&config, rpcMethodHandlerHertz),
		healthChecker:       NewNodeHealthChecker(healthCheckTimeout, maxWaitTime),
		chainVersionRouter:  NewChainVersionRouter(),
		nodeGens:            newNodeGenerations(),
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

			nodeId := nodeIdentity{
				chainId: chainId,
				nodeKey: tempNode.NodeKey,
				native:  tempNode.Source == "native",
			}

			switch changeType {
			case etcd.EVENT_PUT:
				// Each discovery event bumps the node's generation; the
				// health check below only publishes its result if no newer
				// PUT/DELETE arrived while it ran.
				gen := lb.nodeGens.Bump(nodeId)
				// Perform health check in background goroutine
				go func(nodeId nodeIdentity, chainId string, role discovery.NodeType, node *discovery.TargetNode, gen uint64) {
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

					added := lb.nodeGens.ApplyIfCurrent(nodeId, gen, func() {
						_ = lb.NodeSelector.UpsertNode(lb.ctx, chainId, role, targetNode)
					})
					if !added {
						log.Info("discarding stale health check result, node was updated or removed during check",
							log.Any("node_key", node.NodeKey),
							log.Any("address", targetNode.Addr()),
							log.Any("chain_id", chainId))
						return
					}

					log.Info("node health check passed, added to pool",
						log.Any("node_key", node.NodeKey),
						log.Any("address", targetNode.Addr()),
						log.Any("chain_id", chainId))
				}(nodeId, chainId, role, tempNode, gen)

			case etcd.EVENT_DELETE:
				log.Info("removing node from pool",
					log.Any("node_key", tempNode.NodeKey),
					log.Any("address", fmt.Sprintf("%s:%d", tempNode.Address, tempNode.Port)),
					log.Any("chain_id", chainId))
				// Invalidate before anything below can fail: once forgotten,
				// no in-flight health check can re-add the node, and the only
				// path that could re-insert it is a later PUT, which this
				// serial event loop processes after the removal.
				lb.nodeGens.Forget(nodeId)
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
		case version := <-lb.versionChannel:
			lb.handleChainVersionUpdate(version)
		}
	}
}

func (lb *LoadBalancer) handleChainVersionUpdate(update *discovery.ChainVersion) {
	if update == nil {
		return
	}
	version := strings.TrimSpace(update.Version)
	switch update.ChangeType {
	case etcd.EVENT_PUT:
		lb.chainVersionRouter.Update(update.ChainId, version)
		if version == "" {
			log.Info("chain version cleared via empty update",
				log.Any("chain_id", update.ChainId))
		} else {
			log.Info("chain version override updated",
				log.Any("chain_id", update.ChainId),
				log.Any("version", version),
				log.Any("target_chain_id", lb.chainVersionRouter.Resolve(update.ChainId)))
		}
	case etcd.EVENT_DELETE:
		lb.chainVersionRouter.Remove(update.ChainId)
		log.Info("chain version override removed",
			log.Any("chain_id", update.ChainId))
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

func normalizeChainID(chainID string) string {
	chainID = strings.TrimSpace(chainID)
	if chainID == "" {
		return ""
	}

	if chainIDNum, err := parseNumber(chainID); err == nil {
		return fmt.Sprint(chainIDNum)
	}

	return chainID
}

func splitChainIdentifier(chainID string) (string, string, string) {
	trimmed := strings.TrimSpace(chainID)
	if trimmed == "" {
		return "", "", ""
	}

	parts := strings.SplitN(trimmed, "-", 2)
	base := parts[0]
	var uuid string
	if len(parts) == 2 {
		uuid = strings.TrimSpace(parts[1])
	}

	return normalizeChainID(base), uuid, trimmed
}

// ParseBaseChainID returns the numeric base chain ID used by process-wide
// consumers such as usage reporting. Version suffixes are discarded and hex
// chain IDs are normalized in the same way as RPC routing.
func ParseBaseChainID(chainID string) (int64, bool) {
	baseChainID, _, _ := splitChainIdentifier(chainID)
	if baseChainID == "" {
		return 0, false
	}

	numericChainID, err := strconv.ParseInt(baseChainID, 10, 64)
	if err != nil {
		return 0, false
	}
	return numericChainID, true
}

// func (lb *LoadBalancer) ServeHTTP(w http.ResponseWriter, r *http.Request, chainID string) {
func (lb *LoadBalancer) ServeHTTP(ctx context.Context, c *app.RequestContext, chainID string) {
	requestContext := lb.generateRequestContext(ctx, &c.Request)
	if requestContext.Error != nil {
		_, object, _ := ejrpc.BadRequest(requestContext.Error)
		c.JSON(consts.StatusOK, object)
		return
	}

	baseChainID, chainUUID, rawChainID := splitChainIdentifier(chainID)
	if baseChainID == "" {
		_, object, _ := ejrpc.BadRequest(errors.New("invalid chain id"))
		c.JSON(consts.StatusOK, object)
		return
	}

	var effectiveChainID string
	if chainUUID == "" {
		effectiveChainID = lb.chainVersionRouter.Resolve(baseChainID)
		if effectiveChainID != baseChainID {
			log.Debug("chain version override applied",
				log.Any("chain_id", baseChainID),
				log.Any("target_chain_id", effectiveChainID))
		}
	} else {
		effectiveChainID = rawChainID
	}

	requestContext.BaseChainId = baseChainID
	requestContext.ChainUUID = chainUUID
	requestContext.ChainId = effectiveChainID

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

	targetNode, err := lb.chooseTargetNode(requestContext, "")
	if err != nil {
		log.Error("failed to get node, err ", err)
		_, object, retErr := ejrpc.BadGateway(errors.New("no backends available"))
		requestContext.Error = retErr
		requestContext.ResponseBody = object
		c.JSON(consts.StatusOK, object)
		return
	}

	if targetNode.ReverseProxy == nil {
		log.Error("failed to get node, err ", errors.New("no reverse proxy available"))
		_, object, retErr := ejrpc.BadGateway(errors.New("no reverse proxy available"))
		requestContext.Error = retErr
		requestContext.ResponseBody = object
		c.JSON(consts.StatusOK, object)
		return
	}

	log.Debug("Selected target node", log.Any("node", targetNode.Addr()), log.Any("type", targetNode.NodeType), log.Any("chain_id", requestContext.ChainId))

	// If target node is archive, set archive flag
	if targetNode.NodeType == discovery.NodeTypeArchive {
		requestContext.Archive = true
	}

	// First attempt with state node
	lb.attemptRequest(ctx, c, requestContext, targetNode)
	responseBody := c.Response.Body()
	log.Debug("Response status code", log.Any("status_code", c.Response.StatusCode()), log.Any("response_body_size", len(responseBody)))

	responseErrorCodes := lb.parseRPCErrorCodes(responseBody)

	// Check if response contains error code -39006
	if lb.shouldRetryWithArchive(requestContext, responseErrorCodes) {
		log.Info("Received error code -39006(StateBlockNotFound), retrying with archive node")

		// Reset response body for retry
		c.Response.Reset()

		// Retry with archive node
		requestContext.Archive = true

		targetNode, err := lb.chooseTargetNode(requestContext, "")
		if err != nil {
			log.Error("failed to get node, err ", err)
			_, object, retErr := ejrpc.BadGateway(errors.New("no backends available"))
			requestContext.Error = retErr
			requestContext.ResponseBody = object
			c.JSON(consts.StatusOK, object)
			return
		}

		if targetNode.ReverseProxy == nil {
			log.Error("failed to get node, err ", errors.New("no reverse proxy available"))
			_, object, retErr := ejrpc.BadGateway(errors.New("no reverse proxy available"))
			requestContext.Error = retErr
			requestContext.ResponseBody = object
			c.JSON(consts.StatusOK, object)
			return
		}
		log.Debug("Selected archive target node", log.Any("node", targetNode.Addr()), log.Any("chain_id", requestContext.ChainId))
		lb.attemptRequest(ctx, c, requestContext, targetNode)
		responseErrorCodes = lb.parseRPCErrorCodes(c.Response.Body())
	}

	// Retry to nativeNodes when CosmosPrecompile
	if lb.shouldRetryWithNative(requestContext, responseErrorCodes) {
		log.Info("Received error code -39008(CosmosPrecompile), retrying with native node")
		c.Response.Reset()
		requestContext.Native = true

		targetNode, err := lb.chooseTargetNode(requestContext, "native")
		if err != nil {
			log.Error("failed to get native node, err ", err)
			_, object, retErr := ejrpc.BadGateway(errors.New("no native backends available"))
			requestContext.Error = retErr
			requestContext.ResponseBody = object
			c.JSON(consts.StatusOK, object)
			return
		}
		if targetNode.ReverseProxy == nil {
			log.Error("failed to get native node, err ", errors.New("no reverse proxy available"))
			_, object, retErr := ejrpc.BadGateway(errors.New("no reverse proxy available"))
			requestContext.Error = retErr
			requestContext.ResponseBody = object
			c.JSON(consts.StatusOK, object)
			return
		}
		log.Debug("Selected native target node", log.Any("node", targetNode.Addr()), log.Any("chain_id", requestContext.ChainId))
		lb.attemptRequest(ctx, c, requestContext, targetNode)
	}
}

func (lb *LoadBalancer) chooseTargetNode(requestContext *types.RequestContext, requestKey string) (*lbnode.Node, error) {
	if requestContext == nil {
		return nil, errors.New("nil request context")
	}
	requestContext.ClearTargetNode()

	targetNode, err := lb.NodeSelector.GetNode(requestContext, requestKey)
	if err != nil {
		return nil, err
	}
	if targetNode == nil {
		return nil, errors.New("no target node selected")
	}

	requestContext.SetTargetNode(targetNode.Addr())
	return targetNode, nil
}

// upstreamReadTimeout resolves the per-method upstream timeout, falling back
// to the default RPC timeout. Batch requests use the default.
func (lb *LoadBalancer) upstreamReadTimeout(method ejrpc.RPCMethod) time.Duration {
	if t, ok := lb.Config.RPCMethodTimeoutConfig[string(method)]; ok && t > 0 {
		return time.Duration(t) * time.Millisecond
	}
	return time.Duration(lb.Config.DefaultRPCTimeout) * time.Millisecond
}

func (lb *LoadBalancer) attemptRequest(ctx context.Context, c *app.RequestContext, requestContext *types.RequestContext, targetNode *lbnode.Node) {
	if requestContext != nil && requestContext.Native {
		c.Request.URI().SetPath("/")
	}

	// Without a read timeout a hung backend pins its connection forever and
	// eventually exhausts the pool; the request-level option overrides the
	// shared client's default.
	var method ejrpc.RPCMethod
	if requestContext != nil {
		method = requestContext.Method
	}
	c.Request.SetOptions(hzconfig.WithReadTimeout(lb.upstreamReadTimeout(method)))

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
		_, object, retErr := ejrpc.GatewayTimeout(errors.New("reverse proxy gateway timeout"))
		requestContext.Error = retErr
		requestContext.ResponseBody = object
		c.JSON(consts.StatusGatewayTimeout, object)
	}
	if c.Response.StatusCode() == consts.StatusBadGateway {
		_, object, retErr := ejrpc.BadGateway(errors.New("reverse proxy bad gateway"))
		requestContext.Error = retErr
		requestContext.ResponseBody = object
		c.JSON(consts.StatusBadGateway, object)
	}
}

func (lb *LoadBalancer) shouldRetryWithArchive(requestContext *types.RequestContext, responseErrorCodes rpcErrorCodes) bool {
	// If already using archive node, do not retry
	if requestContext == nil || requestContext.Archive {
		return false
	}
	return responseErrorCodes.Has(types.StateBlockNotFound)
}

func (lb *LoadBalancer) shouldRetryWithNative(requestContext *types.RequestContext, responseErrorCodes rpcErrorCodes) bool {
	if requestContext == nil || requestContext.Native {
		return false
	}
	return responseErrorCodes.Has(types.CosmosPrecompile)
}

func (lb *LoadBalancer) rewriteMethodForNativeRetry(c *app.RequestContext, requestContext *types.RequestContext) {
	if c == nil || requestContext == nil {
		return
	}
	if len(requestContext.RequestBody) == 0 {
		return
	}

	rewriteMap := map[ejrpc.RPCMethod]ejrpc.RPCMethod{
		ejrpc.SimulateTransactions: "debank_simulateTransactions",
		ejrpc.ContractMultiCall:    "debank_contractMultiCall",
		ejrpc.EstimateGas:          "debank_estimateGas",
	}

	changed := false
	for _, req := range requestContext.RequestBody {
		if req == nil {
			continue
		}
		if to, ok := rewriteMap[req.Method]; ok {
			req.Method = to
			changed = true
		}
	}
	if !changed {
		return
	}

	var (
		newBody []byte
		err     error
	)
	if requestContext.IsBatch {
		newBody, err = sonic.Marshal(requestContext.RequestBody)
	} else {
		newBody, err = sonic.Marshal(requestContext.RequestBody[0])
		requestContext.Method = requestContext.RequestBody[0].Method
	}
	if err != nil {
		log.Error("failed to rewrite request body for native retry", err)
		return
	}

	requestContext.RawRequestBody = newBody
	requestContext.RequestBodySize = len(newBody)
	c.Request.SetBody(newBody)
}

type rpcErrorCodes map[int]struct{}

func (codes rpcErrorCodes) Has(code int) bool {
	_, ok := codes[code]
	return ok
}

func (lb *LoadBalancer) parseRPCErrorCodes(responseBody []byte) rpcErrorCodes {
	trimmed := bytes.TrimSpace(responseBody)
	if len(trimmed) == 0 {
		return nil
	}

	log.Debug("Response received", log.Any("body_size", len(trimmed)))

	type rpcResponse struct {
		Error *struct {
			Code int `json:"code"`
		} `json:"error"`
	}

	codes := rpcErrorCodes{}

	// batch response
	if len(trimmed) > 0 && trimmed[0] == '[' {
		var resps []rpcResponse
		if err := sonic.Unmarshal(trimmed, &resps); err != nil {
			log.Error("failed to unmarshal batch response, err ", err)
			return nil
		}
		for _, r := range resps {
			if r.Error != nil {
				codes[r.Error.Code] = struct{}{}
			}
		}
		return codes
	}

	var resp rpcResponse
	if err := sonic.Unmarshal(trimmed, &resp); err != nil {
		log.Error("failed to unmarshal response, err ", err)
		return nil
	}
	if resp.Error != nil {
		codes[resp.Error.Code] = struct{}{}
	}
	return codes
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
			continue
		}
		log.Debug("ParseBlockContext", log.Any("method", value.Method), log.Any("params_count", len(arr)))
		if len(arr) <= 0 {
			continue
		}
		blockCtx := arr[len(arr)-1]
		if value.Method == ejrpc.ContractMultiCall || value.Method == ejrpc.SimulateTransactions || value.Method == ejrpc.EstimateGas {
			if len(arr) > 1 {
				blockCtx = arr[1]
			} else {
				blockID := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)
				return &types.BlockContext{
					BlockId: &blockID,
					Type:    "Contains",
				}
			}
		}

		var ctx types.BlockContext
		if err := sonic.Unmarshal(blockCtx, &ctx); err != nil {
			continue
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
	useArchive := isArchiveNodeTypeHeaderValue(request.Header.Get(NodexNodeType))

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
		Archive:         useArchive,
	}
}

func isArchiveNodeTypeHeaderValue(value string) bool {
	return strings.TrimSpace(value) == "archive"
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
