package jsonrpc

import (
	"bytes"
	"context"
	"github.com/Chaintable/nodex-proxy/jsonrpc"
	"github.com/Chaintable/nodex-proxy/lb/jsonrpc/metrics"
	"github.com/Chaintable/nodex-proxy/lib/log"
	"github.com/Chaintable/nodex-proxy/types"
	nJson "github.com/bytedance/sonic"
	"github.com/cloudwego/hertz/pkg/app"
	"go.opentelemetry.io/otel/attribute"
	"net/http"
	"regexp"
	"strings"
	"time"
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
		m := metrics.NewCommonLabelMetrics(processData.Host, processData.Target, processData.ChainId)
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

func requestMirrorHertz(timeout time.Duration, config types.RequestMirrorConfig) types.ProcessorFuncHertz {
	if !config.Enable {
		return nil
	}

	httpClient := http.Client{
		Timeout: timeout,
	}
	doMirrorRequest := func(c *app.RequestContext, processData *types.RequestContext, target string) {
		mirrorRequest, err := http.NewRequest(string(c.Method()), target, bytes.NewReader(processData.RawRequestBody))
		if err != nil {
			log.Error("mirror request error", err)
			return
		}
		mirrorRequest.Header = make(http.Header)
		c.Request.Header.VisitAll(func(key, value []byte) {
			// 为了确保后续使用不会受到影响，复制 key 和 value 的内容
			keyCopy := make([]byte, len(key))
			copy(keyCopy, key)
			valueCopy := make([]byte, len(value))
			copy(valueCopy, value)

			mirrorRequest.Header[string(keyCopy)] = []string{string(valueCopy)}
		})
		mirrorRequest.Host = types.MirrorRequestHost
		resp, err := httpClient.Do(mirrorRequest)
		if err != nil {
			log.Error("mirror request error", err)
			return
		}
		defer resp.Body.Close()
	}
	return func(ctx context.Context, c *app.RequestContext, processData *types.RequestContext) (context.Context, *app.RequestContext, *types.RequestContext) {
		mirrorTarget := types.DynamicRequestMirrorConfig.Target
		if mirrorTarget != "" {
			go doMirrorRequest(c, processData, mirrorTarget)
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
			log.Info("getLatestBlock", log.Any("request", string(rawBytes)))

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
		if c.Response.Body() == nil {
			if !strings.Contains(c.Response.Header.Get("Content-Type"), "application/json") ||
				strings.Contains(c.Response.Header.Get("Transfer-Encoding"), "chunked") {
				return ctx, c, processData
			}
			var (
				body []byte
			)
			body = c.Response.Body()
			if len(body) == 0 {
				return ctx, c, processData
			}
			// skip batch jrpc response
			if jsonrpc.IsBatch(body) {
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
		return ctx, c, processData
	}
}

func logResponseHertz() types.ProcessorFuncHertz {
	return func(ctx context.Context, c *app.RequestContext, processData *types.RequestContext) (context.Context, *app.RequestContext, *types.RequestContext) {
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
		m := metrics.NewCommonLabelMetrics(processData.Host, processData.Target, processData.ChainId)
		responseDuration := time.Since(processData.Start)

		// basic request statistics
		// TotalJRPCRequest = BatchCallsFinished + CallsFinished + CallsFailed
		m.IncrTotalJRPCRequest()
		if processData.IsBatch {
			m.IncrBatchCallsFinished()
			m.ObBatchCallsTime(responseDuration)
			return ctx, c, processData
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
		return ctx, c, processData
	}
}
