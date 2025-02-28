package jsonrpc

import (
	"bytes"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/Chaintable/nodex-proxy/jsonrpc"
	"github.com/Chaintable/nodex-proxy/lb/jsonrpc/metrics"
	"github.com/Chaintable/nodex-proxy/lib/log"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/Chaintable/nodex-proxy/utils"
	nJson "github.com/goccy/go-json"
	"go.opentelemetry.io/otel/attribute"
)

const (
	headerBizKey      = "x-api-biz"
	headerScenarioKey = "x-api-scenario"
	headerDappKey     = "x-api-dapp"
)

func parseJRPCRequestBody() types.PreProcessorFunc {
	return func(request *http.Request, response *http.Response, processData *types.RequestContext) (*http.Request, *http.Response, *types.RequestContext) {
		if processData.RequestBody == nil {
			processData.RawRequestBody, processData.RequestBody, processData.RequestBodySize, processData.Error = jsonrpc.ParseRequest(request)
			if processData.Error != nil {
				response, processData.ResponseBody, processData.Error = jsonrpc.BadRequest(processData.Error)
				return request, response, processData
			}
			processData.IsBatch = len(processData.RequestBody) > 1
			if !processData.IsBatch {
				processData.Method = processData.RequestBody[0].Method
			}
			for _, req := range processData.RequestBody {
				if processData.Error = jsonrpc.ValidateRequest(req); processData.Error != nil {
					response, processData.ResponseBody, processData.Error = jsonrpc.BadRequest(processData.Error)
					break
				}
			}
		}
		return request, response, processData
	}
}

func jRPCMethodDenied(deniedMethods *[]jsonrpc.RPCMethod) types.PreProcessorFunc {
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
	return func(request *http.Request, response *http.Response, processData *types.RequestContext) (*http.Request, *http.Response, *types.RequestContext) {
		for _, req := range processData.RequestBody {
			if _, ok := deniedMethodMap[req.Method]; ok {
				response, processData.ResponseBody, processData.Error = jsonrpc.ErrorResponse(http.StatusBadRequest, errObject, nil)
				break
			}
		}
		return request, response, processData
	}
}

func checkJRPCRequestBody(config types.MethodNameCheckerConfig) types.PreProcessorFunc {
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
	return func(request *http.Request, response *http.Response, processData *types.RequestContext) (*http.Request, *http.Response, *types.RequestContext) {
		for _, req := range processData.RequestBody {
			if deniedRegexp != nil {
				if deniedRegexp.MatchString(string(req.Method)) {
					response, processData.ResponseBody, processData.Error = jsonrpc.ErrorResponse(http.StatusBadRequest, errObject, nil)
					break
				}
			}
			if !reMethodName.MatchString(string(req.Method)) {
				response, processData.ResponseBody, processData.Error = jsonrpc.ErrorResponse(http.StatusBadRequest, errObject, nil)
				break
			}
		}
		return request, response, processData
	}
}

func logRequest() types.PreProcessorFunc {
	return func(request *http.Request, response *http.Response, processData *types.RequestContext) (*http.Request, *http.Response, *types.RequestContext) {
		if types.DebugInfo.DebugModeEnable() {
			log.Observe("RPC Call Started", request.Context(), processData.LogAttributes()...)
		}
		return request, response, processData
	}
}

func updatePreprocessorMetrics() types.PreProcessorFunc {
	return func(request *http.Request, response *http.Response, processData *types.RequestContext) (*http.Request, *http.Response, *types.RequestContext) {
		m := metrics.NewCommonLabelMetrics(processData.Host, processData.Target, processData.ChainId)
		sourceDapp := request.Header.Get(headerDappKey)
		m.IncrCallsStarted(processData.Method, sourceDapp)
		return request, response, processData
	}
}

func rpcMethodLimiter(limiter Limiter) types.PreProcessorFunc {
	return func(request *http.Request, response *http.Response, processData *types.RequestContext) (*http.Request, *http.Response, *types.RequestContext) {
		if !limiter.Allow(processData.Method) {
			response, processData.ResponseBody, processData.Error = jsonrpc.TooManyRequestsResponse()
		}
		return request, response, processData
	}
}

func rpcMethodHandlerProcessor(handlerMap map[jsonrpc.RPCMethod]types.PreProcessorFunc) types.PreProcessorFunc {
	return func(request *http.Request, response *http.Response, processData *types.RequestContext) (*http.Request, *http.Response, *types.RequestContext) {
		if handler, ok := handlerMap[processData.Method]; ok {
			if handler != nil {
				return handler(request, response, processData)
			}
		}
		return request, response, processData
	}
}

func rpcMethodHandlerPostProcessor(handlerMap map[jsonrpc.RPCMethod]types.PostProcessorFunc) types.PostProcessorFunc {
	return func(request *http.Request, response *http.Response, processData *types.RequestContext) *types.RequestContext {
		if handler, ok := handlerMap[processData.Method]; ok {
			if handler != nil {
				return handler(request, response, processData)
			}
		}
		return processData
	}
}

func parseJRPCResponseBody() types.PostProcessorFunc {
	return func(request *http.Request, response *http.Response, processData *types.RequestContext) *types.RequestContext {
		if processData.ResponseBody == nil {
			if !strings.Contains(response.Header.Get("Content-Type"), "application/json") ||
				strings.Contains(response.Header.Get("Transfer-Encoding"), "chunked") {
				return processData
			}
			var (
				body []byte
			)
			body, processData.Error = utils.ReadBodyDataFromHTTPResponse(response)
			if processData.Error != nil {
				log.Error("parseJRPCResponseBody error: ", processData.Error)
				return processData
			}
			if len(body) == 0 {
				return processData
			}
			// skip batch jrpc response
			if jsonrpc.IsBatch(body) {
				return processData
			}

			rawBody := &jsonrpc.RawResponseObject{}
			if processData.Error = nJson.Unmarshal(body, rawBody); processData.Error != nil {
				log.Error(
					"parseJRPCResponseBody error",
					processData.Error,
					log.Any("body", body),
					log.Any("request header", response.Header),
				)
			}
			processData.ResponseBodySize = len(body)

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
		return processData
	}
}

func logResponse() types.PostProcessorFunc {
	return func(request *http.Request, response *http.Response, processData *types.RequestContext) *types.RequestContext {
		if types.DebugInfo.DebugModeEnable() {
			attributes := append(processData.LogAttributes(), log.MillisDurationAttribute("duration", time.Since(processData.Start)))
			log.Observe("RPC Call Complete", request.Context(), attributes...)
		}
		return processData
	}
}

func observabilityLog(config types.ObservabilityLogProcessorConfig) types.PostProcessorFunc {
	if config.Enable {
		return func(request *http.Request, response *http.Response, processData *types.RequestContext) *types.RequestContext {
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
				log.Observe("observability log", request.Context(), attributes...)
			}
			return processData
		}
	}
	return nil
}
func updatePostProcessorMetrics() types.PostProcessorFunc {
	return func(request *http.Request, response *http.Response, processData *types.RequestContext) *types.RequestContext {
		m := metrics.NewCommonLabelMetrics(processData.Host, processData.Target, processData.ChainId)
		responseDuration := time.Since(processData.Start)

		// basic request statistics
		// TotalJRPCRequest = BatchCallsFinished + CallsFinished + CallsFailed
		m.IncrTotalJRPCRequest()
		if processData.IsBatch {
			m.IncrBatchCallsFinished()
			m.ObBatchCallsTime(responseDuration)
			return processData
		} else {
			m.ObCallsTime(processData.Method, responseDuration)
		}

		if processData.Error != nil {
			if processData.ResponseBody != nil && processData.ResponseBody.Error != nil {
				m.IncrCallsFailed(processData.ResponseBody.Error.Code, processData.Method, processData.UpstreamRelated)
			} else {
				log.Error("process data error", processData.Error, processData.LogField())
				m.IncrCallsFailed(jsonrpc.InvalidResponseCode, processData.Method, processData.UpstreamRelated)
			}
		} else {
			m.IncrCallsFinished(processData.Method)
		}

		// size statistics
		if processData.RequestBodySize > 0 {
			m.ObRequestPayloadSizes(processData.Method, processData.RequestBodySize)
		}
		if processData.ResponseBodySize > 0 {
			m.ObResponsePayloadSizes(processData.Method, processData.ResponseBodySize)
		}

		// cache statistics
		if processData.Cached {
			m.IncrCallsCacheHits(processData.Method)
		}
		return processData
	}
}

func paramsCountLimit() types.PreProcessorFunc {
	return func(request *http.Request, response *http.Response, processData *types.RequestContext) (*http.Request, *http.Response, *types.RequestContext) {
		var params []interface{}
		if err := nJson.Unmarshal(processData.RequestBody[0].Params, &params); err != nil {
			return request, response, processData
		}
		if len(params) > 1 {
			response, processData.ResponseBody, processData.Error = jsonrpc.ErrorResponse(http.StatusBadRequest, &jsonrpc.ErrorObject{
				Code:    jsonrpc.ExtTooManyRequestParams,
				Message: jsonrpc.ExtTooManyRequestParamsMsg,
			}, nil)
		}
		return request, response, processData
	}
}

type GeneralRPCMethodHandler struct {
	Config    *types.Config
	HeightMap HeightMap
}

func (e *GeneralRPCMethodHandler) PreHandlerMap() map[jsonrpc.RPCMethod]types.PreProcessorFunc {
	return map[jsonrpc.RPCMethod]types.PreProcessorFunc{
		jsonrpc.GetLatestBlock: getLatestBlock(e.HeightMap),
	}
}

func (e *GeneralRPCMethodHandler) PostHandlerMap() map[jsonrpc.RPCMethod]types.PostProcessorFunc {
	return map[jsonrpc.RPCMethod]types.PostProcessorFunc{}
}

func getLatestBlock(heightMap HeightMap) types.PreProcessorFunc {
	return func(request *http.Request, response *http.Response, processData *types.RequestContext) (*http.Request, *http.Response, *types.RequestContext) {
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
				response, processData.ResponseBody, processData.Error = jsonrpc.ErrorResponse(
					http.StatusInternalServerError,
					&jsonrpc.ErrorObject{
						Code:    jsonrpc.InternalErrorCode,
						Message: "Failed to marshal new request body",
					}, nil,
				)
				return request, response, processData
			}
			io.Copy(io.Discard, request.Body)
			request.Body.Close()
			request.Body = io.NopCloser(bytes.NewReader(rawBytes))
			request.ContentLength = int64(len(rawBytes))
			request.Header.Set("Content-Type", "application/json")
			log.Info("getLatestBlock", log.Any("request", string(rawBytes)))

		} else {
			response, processData.ResponseBody, processData.Error = jsonrpc.ErrorResponse(http.StatusOK, &jsonrpc.ErrorObject{
				Code:    jsonrpc.InvalidParamsCode,
				Message: jsonrpc.InvalidParamsMsg,
			}, nil)
		}
		return request, response, processData
	}
}

func copyRequest(dst *http.Request, src *http.Request) {
	dst.Header = make(http.Header)
	for h, val := range src.Header {
		dst.Header[h] = val
	}
	dst.Host = types.MirrorRequestHost
}

func requestMirror(timeout time.Duration, config types.RequestMirrorConfig) types.PreProcessorFunc {
	if !config.Enable {
		return nil
	}

	httpClient := http.Client{
		Timeout: timeout,
	}
	doMirrorRequest := func(request *http.Request, processData *types.RequestContext, target string) {
		mirrorRequest, err := http.NewRequest(request.Method, target, bytes.NewReader(processData.RawRequestBody))
		if err != nil {
			log.Error("mirror request error", err)
			return
		}
		copyRequest(mirrorRequest, request)
		resp, err := httpClient.Do(mirrorRequest)
		if err != nil {
			log.Error("mirror request error", err)
			return
		}
		defer resp.Body.Close()
	}
	return func(request *http.Request, response *http.Response, processData *types.RequestContext) (*http.Request, *http.Response, *types.RequestContext) {
		mirrorTarget := types.DynamicRequestMirrorConfig.Target
		if mirrorTarget != "" {
			go doMirrorRequest(request, processData, mirrorTarget)
		}
		return request, response, processData
	}
}
