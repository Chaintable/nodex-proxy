package jsonrpc

import (
	"bytes"
	"io"
	"net/http"
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
	return func(c *app.RequestContext, processData *types.RequestContext) {
		for _, req := range processData.RequestBody {
			if _, ok := deniedMethodMap[req.Method]; ok {
				var respObj *jsonrpc.ResponseObject
				respObj, processData.Error = jsonrpc.ErrorResponse(errObject, nil)
				processData.WriteError(c, http.StatusBadRequest, respObj)
				break
			}
		}
	}
}

func updatePreprocessorMetricsHertz() types.ProcessorFuncHertz {
	return func(c *app.RequestContext, processData *types.RequestContext) {
		chainID, chainVersion := metricChainLabels(processData)
		m := metrics.NewCommonLabelMetrics(processData.Host, processData.Target, chainID, chainVersion)
		sourceDapp := c.Request.Header.Get(headerDappKey)
		m.IncrCallsStarted(processData.Method, sourceDapp)
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
	return func(c *app.RequestContext, processData *types.RequestContext) {
		mirrorURLs := mirrorMap.GetMirrorURLs(processData.ChainId)
		if len(mirrorURLs) == 0 {
			return
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

	}
}

// getLatestBlockHertz rewrites getLatestBlock into getBlockByHeight at the
// chain's latest known height.
func getLatestBlockHertz(heightMap HeightMap) types.ProcessorFuncHertz {
	return func(c *app.RequestContext, processData *types.RequestContext) {
		if processData.Method != jsonrpc.GetLatestBlock {
			return
		}
		if height := heightMap.GetHeight(processData.ChainId); height != nil {
			// Keep the caller's id: the upstream echoes whatever we send.
			var id interface{} = 1
			if len(processData.RequestBody) == 1 && processData.RequestBody[0].ID != nil {
				id = nJson.NoCopyRawMessage(processData.RequestBody[0].ID)
			}
			// 构造新的 JSON-RPC 请求体
			requestPayload := map[string]interface{}{
				"jsonrpc": "2.0",
				"method":  "getBlockByHeight", // 目标方法名
				"params": []interface{}{
					height, // 传入的区块高度
				},
				"id": id,
			}

			// 将新请求体序列化为 JSON
			rawBytes, err := nJson.Marshal(requestPayload)
			if err != nil {
				var respObj *jsonrpc.ResponseObject
				respObj, processData.Error = jsonrpc.ErrorResponse(&jsonrpc.ErrorObject{
					Code:    jsonrpc.InternalErrorCode,
					Message: "Failed to marshal new request body",
				}, nil)
				processData.WriteError(c, http.StatusInternalServerError, respObj)

				return
			}
			// SetBody overwrites the buffer the parsed requests borrow from.
			jsonrpc.OwnRequests(processData.RequestBody)
			c.Request.SetBody(rawBytes)
			log.Debug("getLatestBlock request rewritten", log.Any("request_body_size", len(rawBytes)))

		} else {
			var respObj *jsonrpc.ResponseObject
			respObj, processData.Error = jsonrpc.ErrorResponse(&jsonrpc.ErrorObject{
				Code:    jsonrpc.InvalidParamsCode,
				Message: jsonrpc.InvalidParamsMsg,
			}, nil)
			processData.WriteError(c, http.StatusOK, respObj)
		}
	}
}

func parseJRPCResponseBodyHertz() types.ProcessorFuncHertz {
	return func(c *app.RequestContext, processData *types.RequestContext) {
		body := c.Response.Body()
		if len(body) == 0 {
			return
		}
		// Media types are case-insensitive (RFC 9110 §8.3.1); ToLower is
		// alloc-free for the all-lowercase values every known upstream sends.
		if !strings.Contains(strings.ToLower(c.Response.Header.Get("Content-Type")), "application/json") ||
			strings.Contains(strings.ToLower(c.Response.Header.Get("Transfer-Encoding")), "chunked") {
			return
		}
		processData.ResponseBodySize = len(body)
		// skip batch jrpc response
		if jsonrpc.IsBatch(body) {
			return
		}
		// A success response carries no "error" member; skip the unmarshal
		// entirely so the common path stays parse-free.
		if !processData.CachedResponseContainsErrorKey(body) {
			return
		}

		rawBody := &jsonrpc.RawResponseObject{}
		if processData.Error = nJson.Unmarshal(body, rawBody); processData.Error != nil {
			log.Error(
				"parseJRPCResponseBody error",
				processData.Error,
				log.Any("body", body),
				log.Any("request header", string(c.Response.Header.Header())),
			)
			return
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
	}
}

func logResponseHertz() types.ProcessorFuncHertz {
	return func(c *app.RequestContext, processData *types.RequestContext) {
		if log.DebugEnabled() {
			log.Debug("logResponseHertz called",
				log.Any("method", processData.Method),
				log.Any("chain_id", processData.ChainId),
				log.Any("duration_ms", time.Since(processData.Start).Milliseconds()),
				log.Any("status", c.Response.StatusCode()),
				log.Any("has_error", processData.ResponseBody != nil && processData.ResponseBody.Error != nil),
			)
		}
	}
}

func observabilityLogHertz(config types.ObservabilityLogProcessorConfig) types.ProcessorFuncHertz {
	if config.Enable {
		return func(c *app.RequestContext, processData *types.RequestContext) {
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
				log.Observe("observability log", attributes...)
			}
		}
	}
	return nil
}

func updatePostProcessorMetricsHertz() types.ProcessorFuncHertz {
	return func(c *app.RequestContext, processData *types.RequestContext) {
		chainID, chainVersion := metricChainLabels(processData)
		m := metrics.NewCommonLabelMetrics(processData.Host, processData.Target, chainID, chainVersion)
		responseDuration := time.Since(processData.Start)
		method := processData.Method
		if processData.IsBatch {
			method = ""
		}
		bound := m.ForMethod(method)

		// basic request statistics
		// TotalJRPCRequest = BatchCallsFinished + CallsFinished + CallsFailed
		bound.IncrTotalJRPCRequest()

		// Record HTTP status code
		statusCode := c.Response.StatusCode()
		// If status code is 0 (not set), default to 200
		if statusCode == 0 {
			statusCode = 200
		}
		bound.IncrHTTPStatusCode(statusCode)

		if processData.IsBatch {
			bound.IncrBatchCallsFinished()
			bound.ObBatchCallsTime(responseDuration)
			return
		} else {
			bound.ObCallsTime(responseDuration)
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
			bound.IncrCallsFinished()
		}

		// size statistics
		if processData.RequestBodySize > 0 {
			bound.ObRequestPayloadSizes(processData.RequestBodySize)
		}
		responseBodySize := len(c.Response.Body())
		if shouldRecordResponsePayloadSize(statusCode, processData, responseBodySize) {
			bound.ObResponsePayloadSizes(responseBodySize)
		}
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
