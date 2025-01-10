// Copyright (c) 2022 DeBank Inc. <admin@debank.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package jsonrpc

import (
	"net"
	"net/http"
	"time"

	"github.com/Chaintable/nodex-proxy/jsonrpc"
	"github.com/Chaintable/nodex-proxy/types"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// transport used as default reverse-proxy transport that handle
// incoming requests or outgoing responses.
type transport struct {
	requestContext            *types.RequestContext
	defaultHttpTransport      *http.Transport
	rpcMethodHttpTransportMap map[jsonrpc.RPCMethod]*http.Transport

	limiter Limiter

	heightMap HeightMap

	logger *zap.Logger
	config *types.Config

	preProcessor  types.PreProcessorFunc
	postProcessor types.PostProcessorFunc
	props         propagation.TextMapPropagator
}

func GetPreProcessor(config *types.Config, rpcMethodHandler types.RPCMethodHandlerI, limiter Limiter) types.PreProcessorProcessors {
	defaultTimeout := time.Duration(config.DefaultRPCTimeout) * time.Millisecond
	return []types.PreProcessorFunc{
		logRequest(),
		jRPCMethodDenied(config.Processor.MethodDenied),
		checkJRPCRequestBody(config.Processor.MethodNameChecker),
		updatePreprocessorMetrics(),
		rpcMethodLimiter(limiter),
		rpcMethodHandlerProcessor(rpcMethodHandler.PreHandlerMap()),
		requestMirror(defaultTimeout, config.Processor.RequestMirror),
	}
}

func GetPostProcessor(config *types.Config, rpcMethodHandler types.RPCMethodHandlerI) types.PostProcessorProcessors {
	return []types.PostProcessorFunc{
		parseJRPCResponseBody(),
		logResponse(),
		rpcMethodHandlerPostProcessor(rpcMethodHandler.PostHandlerMap()),
		observabilityLog(config.Processor.ObservabilityLog),
		updatePostProcessorMetrics(),
	}
}

func NewTransport(
	requestContext *types.RequestContext,
	limiter Limiter,
	heightMap HeightMap,
	logger *zap.Logger,
	config *types.Config,
	rpcMethodTransportMap map[jsonrpc.RPCMethod]*http.Transport,
	defaultHttpTransport *http.Transport,
	preProcessors types.PreProcessorProcessors,
	postProcessors types.PostProcessorProcessors,
) *transport {
	initTracer(*config)
	return &transport{
		requestContext:            requestContext,
		limiter:                   limiter,
		defaultHttpTransport:      defaultHttpTransport,
		rpcMethodHttpTransportMap: rpcMethodTransportMap,
		logger:                    logger,
		preProcessor:              preProcessors.Call,
		postProcessor:             postProcessors.Call,
		config:                    config,
		props:                     otel.GetTextMapPropagator(),
	}
}

// RoundTrip implements RoundTrip Method for interface of http.RoundTripper.
func (t *transport) RoundTrip(request *http.Request) (response *http.Response, err error) {
	ctx := request.Context()
	ctx, roundTripSpan := tracer.Start(
		ctx,
		"RoundTrip")
	defer roundTripSpan.End()
	processData := t.requestContext
	request, response, processData = t.preProcessor(request, response, processData)
	if response == nil {
		processData.UpstreamRelated = true
		_, upstreamSpan := tracer.Start(
			ctx,
			"Upstream", trace.WithAttributes(attribute.String("rpc_method", string(processData.Method))))
		var (
			httpTransport *http.Transport
			ok            bool
		)
		if httpTransport, ok = t.rpcMethodHttpTransportMap[processData.Method]; !ok {
			httpTransport = t.defaultHttpTransport
		}
		t.props.Inject(ctx, propagation.HeaderCarrier(request.Header))
		response, processData.Error = httpTransport.RoundTrip(request)
		upstreamSpan.End()
		if processData.Error != nil {
			nerr, ok := processData.Error.(net.Error)
			if ok && nerr.Timeout() {
				response, processData.ResponseBody, _ = jsonrpc.GatewayTimeout(processData.Error)
				goto POSTPROCESS
			}
			response, processData.ResponseBody, _ = jsonrpc.BadGateway(processData.Error)
		}
	}

POSTPROCESS:
	processData = t.postProcessor(request, response, processData)
	return response, nil
}
