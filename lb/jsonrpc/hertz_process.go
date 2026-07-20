package jsonrpc

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/Chaintable/nodex-proxy/jsonrpc"
	"github.com/Chaintable/nodex-proxy/lb/jsonrpc/metrics"
	"github.com/Chaintable/nodex-proxy/lib/log"
	"github.com/Chaintable/nodex-proxy/types"
	nJson "github.com/bytedance/sonic"
	"github.com/cloudwego/hertz/pkg/app"
	"go.opentelemetry.io/otel/attribute"
)

const (
	headerBizKey      = "x-api-biz"
	headerScenarioKey = "x-api-scenario"
	headerDappKey     = "x-api-dapp"
)

func logRequestHertz() types.ProcessorFuncHertz {
	return func(ctx context.Context, c *app.RequestContext, processData *types.RequestContext) (context.Context, *app.RequestContext, *types.RequestContext) {
		if types.DebugInfo.DebugModeEnable() {
			log.Observe("RPC Call Started", ctx, processData.LogAttributes()...)
		}
		return ctx, c, processData
	}
}

func jRPCMethodDeniedHertz(deniedMethods *[]jsonrpc.RPCMethod) types.ProcessorFuncHertz {
	deniedMethodMap := map[jsonrpc.RPCMethod]struct{}{}
	if deniedMethods != nil {
		for _, method := range *deniedMethods {
			deniedMethodMap[method] = struct{}{}
		}
	}

	errObject := &jsonrpc.ErrorObject{
		Code:    jsonrpc.ExtMethodNotAllowed,
		Message: jsonrpc.ExtMethodNotAllowedMsg,
	}
	return func(ctx context.Context, c *app.RequestContext, processData *types.RequestContext) (context.Context, *app.RequestContext, *types.RequestContext) {
		for _, req := range processData.RequestBody {
			if _, ok := deniedMethodMap[req.Method]; ok {
				var respObj *jsonrpc.ResponseObject
				_, respObj, processData.Error = jsonrpc.ErrorResponse(http.StatusBadRequest, errObject, nil)
				c.JSON(http.StatusBadRequest, respObj)
				break
			}
		}
		return ctx, c, processData
	}
}

func checkJRPCRequestBodyHertz(config types.MethodNameCheckerConfig) types.ProcessorFuncHertz {
	if !config.Enable {
		return nil
	}

	reMethodName := regexp.MustCompile(config.Regexp)
	var deniedRegexp *regexp.Regexp
	if config.DeniedRegexp != "" {
		deniedRegexp = regexp.MustCompile(config.DeniedRegexp)
	}
	errObject := &jsonrpc.ErrorObject{
		Code:    jsonrpc.ExtMethodNotAllowed,
		Message: jsonrpc.ExtMethodNotAllowedMsg,
	}
	return func(ctx context.Context, c *app.RequestContext, processData *types.RequestContext) (context.Context, *app.RequestContext, *types.RequestContext) {
		for _, req := range processData.RequestBody {
			if deniedRegexp != nil {
				if deniedRegexp.MatchString(string(req.Method)) {
					var respObj *jsonrpc.ResponseObject
					_, respObj, processData.Error = jsonrpc.ErrorResponse(http.StatusBadRequest, errObject, nil)
					c.JSON(http.StatusBadRequest, respObj)
					break
				}
			}
			if !reMethodName.MatchString(string(req.Method)) {
				var respObj *jsonrpc.ResponseObject
				_, respObj, processData.Error = jsonrpc.ErrorResponse(http.StatusBadRequest, errObject, nil)
				c.JSON(http.StatusBadRequest, respObj)
				break
			}
		}
		return ctx, c, processData
	}
}

func updatePreprocessorMetricsHertz() types.ProcessorFuncHertz {
	return func(ctx context.Context, c *app.RequestContext, processData *types.RequestContext) (context.Context, *app.RequestContext, *types.RequestContext) {
		chainID, chainVersion := metricChainLabels(processData)
		m := metrics.NewCommonLabelMetrics(processData.Host, processData.Target, chainID, chainVersion)
		sourceDapp := c.Request.Header.Get(headerDappKey)
		m.IncrCallsStarted(processData.Method, sourceDapp)
		return ctx, c, processData
	}
}

func rpcMethodLimiterHertz(limiter Limiter) types.ProcessorFuncHertz {
	return func(ctx context.Context, c *app.RequestContext, processData *types.RequestContext) (context.Context, *app.RequestContext, *types.RequestContext) {
		if !limiter.Allow(processData.Method) {
			var respObj *jsonrpc.ResponseObject
			_, respObj, processData.Error = jsonrpc.TooManyRequestsResponse()
			c.JSON(http.StatusTooManyRequests, respObj)
		}
		return ctx, c, processData
	}
}

func rpcMethodHandlerProcessorHertz(handlerMap map[jsonrpc.RPCMethod]types.ProcessorFuncHertz) types.ProcessorFuncHertz {
	return func(ctx context.Context, c *app.RequestContext, processData *types.RequestContext) (context.Context, *app.RequestContext, *types.RequestContext) {
		if handler, ok := handlerMap[processData.Method]; ok {
			if handler != nil {
				return handler(ctx, c, processData)
			}
		}
		return ctx, c, processData
	}
}

func requestMirrorHertz(timeout time.Duration, config types.RequestMirrorConfig, mirrorMap MirrorMap, mirrorLimiter MirrorLimiter) types.ProcessorFuncHertz {
	if !config.Enable {
		return nil
	}

	httpClient := http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			// no global idle cap: with hundreds of mirror hosts a small cap
			// evicts other hosts' idle conns and reintroduces churn; idle
			// conns are reclaimed by IdleConnTimeout instead
			MaxIdleConns:        0,
			MaxIdleConnsPerHost: 32,
			MaxConnsPerHost:     128,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	doMirrorRequest := func(method string, header http.Header, body []byte, target string) {
		mirrorRequest, err := http.NewRequest(method, target, bytes.NewReader(body))
		if err != nil {
			log.Error("mirror request creation error", err, log.Any("target", target))
			return
		}
		mirrorRequest.Header = header.Clone()
		mirrorRequest.Host = types.MirrorRequestHost

		resp, err := httpClient.Do(mirrorRequest)
		if err != nil {
			return
		}
		defer resp.Body.Close()
		// The connection only goes back to the pool after the body is read to
		// EOF; without draining every mirror request dials a new connection.
		_, _ = io.Copy(io.Discard, resp.Body)
	}
	return func(ctx context.Context, c *app.RequestContext, processData *types.RequestContext) (context.Context, *app.RequestContext, *types.RequestContext) {
		mirrorURLs := mirrorMap.GetMirrorURLs(processData.ChainId)
		if len(mirrorURLs) == 0 {
			return ctx, c, processData
		}

		// Snapshot method/headers/body on the handler goroutine: c and
		// processData.RawRequestBody alias pooled hertz buffers that are
		// recycled for the next request once the handler returns, so the
		// async mirror goroutines must only ever see independent copies.
		method := string(c.Method())
		header := make(http.Header)
		c.Request.Header.VisitAll(func(key, value []byte) {
			header.Add(string(key), string(value))
		})
		body := make([]byte, len(processData.RawRequestBody))
		copy(body, processData.RawRequestBody)
		chainId := processData.ChainId

		for _, url := range mirrorURLs {
			if !mirrorLimiter.Allow(chainId, url) {
				continue
			}
			go doMirrorRequest(method, header, body, url)
		}

		return ctx, c, processData
	}
}

type GeneralRPCMethodHertzHandler struct {
	Config    *types.Config
	HeightMap HeightMap
}

func (e *GeneralRPCMethodHertzHandler) PreHandlerMap() map[jsonrpc.RPCMethod]types.ProcessorFuncHertz {
	return map[jsonrpc.RPCMethod]types.ProcessorFuncHertz{
		jsonrpc.GetLatestBlock: getLatestBlockHertz(e.HeightMap),
	}
}

func (e *GeneralRPCMethodHertzHandler) PostHandlerMap() map[jsonrpc.RPCMethod]types.ProcessorFuncHertz {
	return map[jsonrpc.RPCMethod]types.ProcessorFuncHertz{}
}

func getLatestBlockHertz(heightMap HeightMap) types.ProcessorFuncHertz {
	return func(ctx context.Context, c *app.RequestContext, processData *types.RequestContext) (context.Context, *app.RequestContext, *types.RequestContext) {
		if height := heightMap.GetHeight(processData.ChainId); height != nil {
			// 构造新的 JSON-RPC 请求体
			requestPayload := map[string]interface{}{
				"jsonrpc": "2.0",
				"method":  "getBlockByHeight", // 目标方法名
				"params": []interface{}{
					height, // 传入的区块高度
				},
				"id": 1, // 可根据实际情况设置
			}

			// 将新请求体序列化为 JSON
			rawBytes, err := nJson.Marshal(requestPayload)
			if err != nil {
				var respObj *jsonrpc.ResponseObject
				_, respObj, processData.Error = jsonrpc.ErrorResponse(
					http.StatusInternalServerError,
					&jsonrpc.ErrorObject{
						Code:    jsonrpc.InternalErrorCode,
						Message: "Failed to marshal new request body",
					}, nil,
				)
				c.JSON(http.StatusInternalServerError, respObj)

				return ctx, c, processData
			}
			c.Request.SetBody(rawBytes)
			log.Debug("getLatestBlock request rewritten", log.Any("request_body_size", len(rawBytes)))

		} else {
			var respObj *jsonrpc.ResponseObject
			_, respObj, processData.Error = jsonrpc.ErrorResponse(http.StatusOK, &jsonrpc.ErrorObject{
				Code:    jsonrpc.InvalidParamsCode,
				Message: jsonrpc.InvalidParamsMsg,
			}, nil)
			c.JSON(http.StatusOK, respObj)
		}
		return ctx, c, processData
	}
}

func parseJRPCResponseBodyHertz() types.ProcessorFuncHertz {
	return func(ctx context.Context, c *app.RequestContext, processData *types.RequestContext) (context.Context, *app.RequestContext, *types.RequestContext) {
		body := c.Response.Body()
		if len(body) == 0 {
			return ctx, c, processData
		}
		// Media types are case-insensitive (RFC 9110 §8.3.1); ToLower is
		// alloc-free for the all-lowercase values every known upstream sends.
		if !strings.Contains(strings.ToLower(c.Response.Header.Get("Content-Type")), "application/json") ||
			strings.Contains(strings.ToLower(c.Response.Header.Get("Transfer-Encoding")), "chunked") {
			return ctx, c, processData
		}
		processData.ResponseBodySize = len(body)
		// skip batch jrpc response
		if jsonrpc.IsBatch(body) {
			return ctx, c, processData
		}
		// A success response carries no "error" member; skip the unmarshal
		// entirely so the common path stays parse-free.
		if !jsonrpc.ContainsErrorKey(body) {
			return ctx, c, processData
		}

		rawBody := &jsonrpc.RawResponseObject{}
		if processData.Error = nJson.Unmarshal(body, rawBody); processData.Error != nil {
			log.Error(
				"parseJRPCResponseBody error",
				processData.Error,
				log.Any("body", body),
				log.Any("request header", string(c.Response.Header.Header())),
			)
			return ctx, c, processData
		}

		processData.ResponseBody = &jsonrpc.ResponseObject{
			Jsonrpc: rawBody.Jsonrpc,
			Error:   rawBody.Error,
			Result:  rawBody.Result,
			ID:      rawBody.ID,
		}
		if processData.ResponseBody.Error != nil {
			processData.Error = processData.ResponseBody.Error
		}
		return ctx, c, processData
	}
}

func logResponseHertz() types.ProcessorFuncHertz {
	return func(ctx context.Context, c *app.RequestContext, processData *types.RequestContext) (context.Context, *app.RequestContext, *types.RequestContext) {
		if log.DebugEnabled() {
			log.Debug("logResponseHertz called",
				log.Any("method", processData.Method),
				log.Any("chain_id", processData.ChainId),
				log.Any("duration_ms", time.Since(processData.Start).Milliseconds()),
				log.Any("status", c.Response.StatusCode()),
				log.Any("has_error", processData.ResponseBody != nil && processData.ResponseBody.Error != nil),
			)
		}
		if types.DebugInfo.DebugModeEnable() {
			attributes := append(processData.LogAttributes(), log.MillisDurationAttribute("duration", time.Since(processData.Start)))
			log.Observe("RPC Call Complete", ctx, attributes...)
		}
		return ctx, c, processData
	}
}

func rpcMethodHandlerPostProcessorHertz(handlerMap map[jsonrpc.RPCMethod]types.ProcessorFuncHertz) types.ProcessorFuncHertz {
	return func(ctx context.Context, c *app.RequestContext, processData *types.RequestContext) (context.Context, *app.RequestContext, *types.RequestContext) {
		if handler, ok := handlerMap[processData.Method]; ok {
			if handler != nil {
				return handler(ctx, c, processData)
			}
		}
		return ctx, c, processData
	}
}

func observabilityLogHertz(config types.ObservabilityLogProcessorConfig) types.ProcessorFuncHertz {
	if config.Enable {
		return func(ctx context.Context, c *app.RequestContext, processData *types.RequestContext) (context.Context, *app.RequestContext, *types.RequestContext) {
			duration := time.Since(processData.Start)
			observe := duration >= time.Duration(config.SlowThreshold.Default)*time.Millisecond
			if threshold, ok := config.SlowThreshold.RpcMethods[processData.Method]; ok {
				observe = duration >= time.Duration(threshold)*time.Millisecond
			}
			if (config.EnableErrorLog && processData.ResponseBody != nil && processData.ResponseBody.Error != nil) || (observe) {
				attributes := append(processData.LogAttributes(), log.MillisDurationAttribute("duration", time.Since(processData.Start)))
				if processData.ResponseBody != nil && processData.ResponseBody.Error != nil {
					attributes = append(attributes, attribute.Int("ErrorCode", int(processData.ResponseBody.Error.Code)))
					attributes = append(attributes, attribute.String("ErrorMessage", string(processData.ResponseBody.Error.Message)))
				}
				log.Observe("observability log", ctx, attributes...)
			}
			return ctx, c, processData
		}
	}
	return nil
}

func updatePostProcessorMetricsHertz() types.ProcessorFuncHertz {
	return func(ctx context.Context, c *app.RequestContext, processData *types.RequestContext) (context.Context, *app.RequestContext, *types.RequestContext) {
		chainID, chainVersion := metricChainLabels(processData)
		m := metrics.NewCommonLabelMetrics(processData.Host, processData.Target, chainID, chainVersion)
		responseDuration := time.Since(processData.Start)

		// basic request statistics
		// TotalJRPCRequest = BatchCallsFinished + CallsFinished + CallsFailed
		m.IncrTotalJRPCRequest()

		// Record HTTP status code
		statusCode := c.Response.StatusCode()
		// If status code is 0 (not set), default to 200
		if statusCode == 0 {
			statusCode = 200
		}
		if processData.IsBatch {
			m.IncrHTTPStatusCode(statusCode, "")
		} else {
			m.IncrHTTPStatusCode(statusCode, processData.Method)
		}

		if processData.IsBatch {
			m.IncrBatchCallsFinished()
			m.ObBatchCallsTime(responseDuration)
			return ctx, c, processData
		} else {
			m.ObCallsTime(processData.Method, responseDuration)
		}

		if processData.Error != nil {
			nodeAddr := failureNodeAddr(processData)
			if processData.ResponseBody != nil && processData.ResponseBody.Error != nil {
				reason := classifyFailureReason(
					processData.ResponseBody.Error.Code,
					processData.ResponseBody.Error.Message,
					processData.ResponseBody.Error.Data,
					processData.Error,
				)
				m.IncrCallsFailed(processData.ResponseBody.Error.Code, processData.Method, processData.UpstreamRelated, reason, nodeAddr)
			} else {
				log.Error("process data error", processData.Error, processData.LogField())
				reason := classifyFailureReason(jsonrpc.InvalidResponseCode, "", nil, processData.Error)
				m.IncrCallsFailed(jsonrpc.InvalidResponseCode, processData.Method, processData.UpstreamRelated, reason, nodeAddr)
			}
		} else {
			m.IncrCallsFinished(processData.Method)
		}

		// size statistics
		if processData.RequestBodySize > 0 {
			m.ObRequestPayloadSizes(processData.Method, processData.RequestBodySize)
		}
		responseBodySize := len(c.Response.Body())
		if shouldRecordResponsePayloadSize(statusCode, processData, responseBodySize) {
			m.ObResponsePayloadSizes(processData.Method, responseBodySize)
		}

		// cache statistics
		if processData.Cached {
			m.IncrCallsCacheHits(processData.Method)
		}
		return ctx, c, processData
	}
}

func shouldRecordResponsePayloadSize(statusCode int, processData *types.RequestContext, responseBodySize int) bool {
	if processData == nil || processData.Error != nil || responseBodySize <= 0 {
		return false
	}
	return statusCode >= http.StatusOK && statusCode < http.StatusMultipleChoices
}

func failureNodeAddr(processData *types.RequestContext) string {
	if processData == nil || strings.TrimSpace(processData.TargetNodeAddr) == "" {
		return "unknown"
	}
	return processData.TargetNodeAddr
}

func classifyFailureReason(code jsonrpc.ErrorCode, msg jsonrpc.ErrorMsg, data interface{}, fallbackErr error) string {
	rawData := strings.ToLower(extractErrorDataString(data))
	rawMsg := strings.ToLower(string(msg))
	rawErr := ""
	if fallbackErr != nil {
		rawErr = strings.ToLower(fallbackErr.Error())
	}

	// Keep buckets finite but detailed for operational diagnosis.
	switch code {
	case jsonrpc.ExtTooManyRequests:
		return "rate_limited"
	case jsonrpc.ExtMethodNotAllowed:
		return "method_not_allowed"
	case jsonrpc.InvalidParamsCode:
		return "invalid_params"
	case jsonrpc.InvalidRequestCode:
		return "invalid_request"
	case jsonrpc.ParseErrorCode:
		return "parse_error"
	case jsonrpc.InvalidResponseCode:
		if strings.Contains(rawErr, "unmarshal") {
			return "response_unmarshal_failed"
		}
		if strings.Contains(rawErr, "timeout") {
			return "response_parse_timeout"
		}
		return "invalid_response"
	case jsonrpc.ExtWaitingTargetResponseTimeout:
		return "upstream_gateway_timeout"
	case jsonrpc.InternalErrorCode:
		if strings.Contains(rawData, "no native backends available") {
			return "no_native_backends_available"
		}
		if strings.Contains(rawData, "no backends available") {
			return "no_backends_available"
		}
		if strings.Contains(rawData, "no reverse proxy available") {
			return "no_reverse_proxy_available"
		}
		if strings.Contains(rawData, "reverse proxy bad gateway") {
			return "upstream_bad_gateway"
		}
		if strings.Contains(rawData, "reverse proxy gateway timeout") {
			return "upstream_gateway_timeout"
		}
		if strings.Contains(rawData, "marshal") {
			return "request_transform_failed"
		}
		if strings.Contains(rawMsg, "server error") {
			return "server_error_other"
		}
		return "internal_error_other"
	default:
		if strings.Contains(rawErr, "timeout") || strings.Contains(rawData, "timeout") {
			return "timeout_other"
		}
		return "rpc_error_other"
	}
}

func extractErrorDataString(data interface{}) string {
	switch v := data.(type) {
	case nil:
		return ""
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return ""
	}
}

func metricChainLabels(processData *types.RequestContext) (string, string) {
	if processData == nil {
		return "", ""
	}

	baseChainID := strings.TrimSpace(processData.BaseChainId)
	if baseChainID == "" {
		baseChainID = strings.TrimSpace(processData.ChainId)
	}

	chainVersion := ""
	if processData.ChainUUID != "" {
		chainVersion = processData.ChainUUID
	} else if baseChainID != "" && processData.ChainId != "" && processData.ChainId != baseChainID {
		prefix := baseChainID + "-"
		if strings.HasPrefix(processData.ChainId, prefix) {
			chainVersion = strings.TrimPrefix(processData.ChainId, prefix)
		}
	}

	return baseChainID, chainVersion
}
