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
	"time"

	"github.com/Chaintable/nodex-proxy/types"
)

func GetPreProcessorHertz(config *types.Config, rpcMethodHandler types.RPCMethodHandlerIHertz, limiter Limiter, mirrorMap MirrorMap, mirrorLimiter MirrorLimiter) types.PreProcessorProcessorsHertz {
	defaultTimeout := time.Duration(config.DefaultRPCTimeout) * time.Millisecond
	return []types.ProcessorFuncHertz{
		logRequestHertz(),
		jRPCMethodDeniedHertz(config.Processor.MethodDenied),
		checkJRPCRequestBodyHertz(config.Processor.MethodNameChecker),
		updatePreprocessorMetricsHertz(),
		rpcMethodLimiterHertz(limiter),
		rpcMethodHandlerProcessorHertz(rpcMethodHandler.PreHandlerMap()),
		requestMirrorHertz(defaultTimeout, config.Processor.RequestMirror, mirrorMap, mirrorLimiter),
	}
}

func GetPostProcessorHertz(config *types.Config, rpcMethodHandler types.RPCMethodHandlerIHertz) types.PostProcessorProcessorsHertz {
	return []types.ProcessorFuncHertz{
		parseJRPCResponseBodyHertz(),
		logResponseHertz(),
		rpcMethodHandlerPostProcessorHertz(rpcMethodHandler.PostHandlerMap()),
		observabilityLogHertz(config.Processor.ObservabilityLog),
		updatePostProcessorMetricsHertz(),
	}
}
