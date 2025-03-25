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
	"context"
	"os"
	"time"

	"github.com/Chaintable/nodex-proxy/lib/log"
	"github.com/Chaintable/nodex-proxy/types"
	"go.opentelemetry.io/contrib/propagators/aws/xray"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"google.golang.org/grpc"
)

var Tracer = otel.Tracer("jrpcx")

func initTracer(config types.Config) error {
	traceConfig := config.Observability.Trace

	idg := xray.NewIDGenerator()
	attrs := []attribute.KeyValue{
		// the service name used to display traces in backends
		semconv.ServiceNameKey.String(config.ServiceName),
	}
	for k, v := range config.Observability.StaticResource {
		attrs = append(attrs, attribute.String(k, v))
	}

	// k8s
	// https://opentelemetry.io/docs/reference/specification/resource/semantic_conventions/k8s/
	if envValue := os.Getenv("K8S_CLUSTER_NAME"); envValue != "" {
		attrs = append(attrs, semconv.K8SClusterNameKey.String(envValue))
	}
	if envValue := os.Getenv("K8S_POD_NAMESPACE"); envValue != "" {
		attrs = append(attrs, semconv.K8SNamespaceNameKey.String(envValue))
	}
	if envValue := os.Getenv("K8S_POD_NAME"); envValue != "" {
		attrs = append(attrs, semconv.K8SPodNameKey.String(envValue))
	}
	if envValue := os.Getenv("K8S_POD_UID"); envValue != "" {
		attrs = append(attrs, semconv.K8SPodUIDKey.String(envValue))
	}
	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		attrs...,
	)
	tracerProviderOptions := []sdktrace.TracerProviderOption{
		sdktrace.WithSampler(sdktrace.ParentBased(awsXRAYTraceIDRatioBased(traceConfig.SamplingRatio))),
		sdktrace.WithResource(res),

		sdktrace.WithIDGenerator(idg),
	}
	if traceConfig.Enable {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// Create and start new OTLP trace exporter
		traceExporter, err := otlptracegrpc.New(
			ctx, otlptracegrpc.WithInsecure(),
			otlptracegrpc.WithEndpoint(traceConfig.OTLPEndpoint),
			otlptracegrpc.WithDialOption(grpc.WithBlock()))
		if err != nil {
			log.Error(" new oltp exporter error", err)
		}
		tracerProviderOptions = append(tracerProviderOptions, sdktrace.WithBatcher(traceExporter))

	}
	// https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/sdk.md
	tp := sdktrace.NewTracerProvider(tracerProviderOptions...)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(xray.Propagator{})
	return nil
}
