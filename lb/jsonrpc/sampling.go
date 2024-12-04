package jsonrpc

import (
	"encoding/binary"
	"fmt"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func awsXRAYTraceIDRatioBased(fraction float64) sdktrace.Sampler {
	if fraction >= 1 {
		return sdktrace.AlwaysSample()
	}

	if fraction <= 0 {
		fraction = 0
	}

	return awsXRAYTraceIDRatioSampler{
		traceIDUpperBound: uint64(fraction * (1 << 63)),
		description:       fmt.Sprintf("TraceIDRatioBased{%g}", fraction),
	}
}

type awsXRAYTraceIDRatioSampler struct {
	traceIDUpperBound uint64
	description       string
}

func (ts awsXRAYTraceIDRatioSampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	psc := trace.SpanContextFromContext(p.ParentContext)
	x := binary.BigEndian.Uint64(p.TraceID[4:12]) >> 1
	if x < ts.traceIDUpperBound {
		return sdktrace.SamplingResult{
			Decision:   sdktrace.RecordAndSample,
			Tracestate: psc.TraceState(),
		}
	}
	return sdktrace.SamplingResult{
		Decision:   sdktrace.Drop,
		Tracestate: psc.TraceState(),
	}
}

func (ts awsXRAYTraceIDRatioSampler) Description() string {
	return ts.description
}
