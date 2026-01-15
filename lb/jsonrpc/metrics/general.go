package metrics

import (
	"strconv"
	"time"

	"github.com/Chaintable/nodex-proxy/jsonrpc"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/go-kit/kit/metrics/prometheus"
	stdprome "github.com/prometheus/client_golang/prometheus"
)

const (
	promNamespace = "jrpcx"
	promSubsystem = "rpc"
)

func newLabelNames(labelNames []string, name ...string) []string {
	n := make([]string, len(labelNames))
	copy(n, labelNames)
	return append(n, name...)
}

var (
	commonLabelNames                 = []string{"host", "target", "chain_id", "chain_version"}
	methodCommonLabelNames           = newLabelNames(commonLabelNames, "method")
	methodSourceDappCommonLabelNames = newLabelNames(methodCommonLabelNames, "sourcedapp")
)

type CommonLabelMetrics struct {
	labelValues []string
}

func NewCommonLabelMetrics(host types.ProcessorHost, target types.ProcessorTarget, chainID, chainVersion string) CommonLabelMetrics {
	return CommonLabelMetrics{
		labelValues: []string{
			"host", string(host),
			"target", string(target),
			"chain_id", chainID,
			"chain_version", chainVersion,
		},
	}
}

func (m CommonLabelMetrics) IncrCallsStarted(method jsonrpc.RPCMethod, sourcedapp string) {
	callsStarted.With(m.labelValues...).With("method", string(method), "sourcedapp", sourcedapp).Add(1)
}

func (m CommonLabelMetrics) IncrCallsFinished(method jsonrpc.RPCMethod) {
	callsFinished.With(m.labelValues...).With("method", string(method)).Add(1)
}

func (m CommonLabelMetrics) IncrBatchCallsFinished() {
	batchCallsFinished.With(m.labelValues...).Add(1)
}

func (m CommonLabelMetrics) IncrTotalJRPCRequest() {
	totalJRPCRequest.With(m.labelValues...).Add(1)
}

func (m CommonLabelMetrics) IncrCallsFailed(code jsonrpc.ErrorCode, method jsonrpc.RPCMethod, upstreamRelated bool) {
	callsFailed.With(m.labelValues...).With(
		"status_code", strconv.FormatInt(int64(code), 10),
		"method", string(method),
		"upstream_related", strconv.FormatBool(upstreamRelated),
	).Add(1)
}

func (m CommonLabelMetrics) IncrCallsCacheHits(method jsonrpc.RPCMethod) {
	callsCacheHits.With(m.labelValues...).With("method", string(method)).Add(1)
}

// ObCallsTime observe time cost in milliseconds for a single rpc call.
func (m CommonLabelMetrics) ObCallsTime(method jsonrpc.RPCMethod, duration time.Duration) {
	callsTime.With(m.labelValues...).With("method", string(method)).Observe(float64(duration.Milliseconds()))
}

// ObBatchCallsTime observe time cost in milliseconds for a single rpc call.
func (m CommonLabelMetrics) ObBatchCallsTime(duration time.Duration) {
	batchCallsTime.With(m.labelValues...).Observe(float64(duration.Milliseconds()))
}

// ObRequestPayloadSizes observe payload size for a single rpc call.
func (m CommonLabelMetrics) ObRequestPayloadSizes(method jsonrpc.RPCMethod, size int) {
	requestPayloadSizes.With(m.labelValues...).With("method", string(method)).Observe(float64(size))
}

// ObResponsePayloadSizes observe payload size for a single rpc call.
func (m CommonLabelMetrics) ObResponsePayloadSizes(method jsonrpc.RPCMethod, size int) {
	responsePayloadSizes.With(m.labelValues...).With("method", string(method)).Observe(float64(size))
}

func (m CommonLabelMetrics) IncrInternalFailedRequest() {
	internalFailedRequest.With(m.labelValues...).Add(1)
}

// IncrHTTPStatusCode increments the HTTP status code counter
func (m CommonLabelMetrics) IncrHTTPStatusCode(statusCode int, method jsonrpc.RPCMethod) {
	if method != "" {
		httpStatusCode.With(m.labelValues...).With("method", string(method), "status_code", strconv.Itoa(statusCode)).Add(1)
	} else {
		httpStatusCode.With(m.labelValues...).With("method", "batch", "status_code", strconv.Itoa(statusCode)).Add(1)
	}
}

var (
	callsStarted = prometheus.NewCounterFrom(stdprome.CounterOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "calls_started",
		Help:      "Number of received RPC calls (unique un-batched requests)",
	}, methodSourceDappCommonLabelNames)
	callsFinished = prometheus.NewCounterFrom(stdprome.CounterOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "calls_finished",
		Help:      "Number of processed RPC calls (unique un-batched requests)",
	}, methodCommonLabelNames)
	callsCacheHits = prometheus.NewCounterFrom(stdprome.CounterOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "calls_cache_hits",
		Help:      "Number of hit the cache RPC calls (unique un-batched requests)",
	}, methodCommonLabelNames)
	callsFailed = prometheus.NewCounterFrom(stdprome.CounterOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "calls_failed",
		Help:      "Number of failure RPC calls (unique un-batched requests)",
	}, newLabelNames(methodCommonLabelNames, "status_code", "upstream_related"))
	batchCallsFinished = prometheus.NewCounterFrom(stdprome.CounterOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "batch_calls_finished",
		Help:      "Number of processed RPC batch calls",
	}, commonLabelNames)
	callsTime = prometheus.NewHistogramFrom(stdprome.HistogramOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "calls_time",
		Help:      "Request duration in milliseconds of RPC calls",
		Buckets:   []float64{1, 5, 10, 25, 50, 100, 300, 500, 1000, 3000, 5000, 10000},
	}, methodCommonLabelNames)
	batchCallsTime = prometheus.NewHistogramFrom(stdprome.HistogramOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "batch_calls_time",
		Help:      "Request duration in milliseconds of RPC batch calls",
		Buckets:   []float64{1, 5, 10, 25, 50, 100, 300, 500, 1000, 3000, 5000, 10000},
	}, commonLabelNames)
	totalJRPCRequest = prometheus.NewCounterFrom(stdprome.CounterOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "request",
		Help:      "Total request count",
	}, commonLabelNames)
	requestPayloadSizes = prometheus.NewHistogramFrom(stdprome.HistogramOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "request_payload_sizes",
		Help:      "Histogram of RPC request payload sizes",
		Buckets:   []float64{10, 50, 100, 500, 1_000, 5_000, 10_000, 100_000, 1_000_000},
	}, methodCommonLabelNames)
	responsePayloadSizes = prometheus.NewHistogramFrom(stdprome.HistogramOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "response_payload_sizes",
		Help:      "Histogram of RPC response payload sizes",
		Buckets:   []float64{10, 50, 100, 500, 1_000, 5_000, 10_000, 100_000, 1_000_000},
	}, methodCommonLabelNames)
	internalFailedRequest = prometheus.NewCounterFrom(stdprome.CounterOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "internal_failed_request",
		Help:      "Number of failed internal requests",
	}, commonLabelNames)
	httpStatusCode = prometheus.NewCounterFrom(stdprome.CounterOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "http_status_code",
		Help:      "HTTP status code counter",
	}, newLabelNames(commonLabelNames, "method", "status_code"))
	nodeHealthCheckTotal = prometheus.NewCounterFrom(stdprome.CounterOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "health_check_total",
		Help:      "Total number of node health checks performed",
	}, []string{"chain_id", "node_key", "status"})
	nodeHealthCheckDuration = prometheus.NewHistogramFrom(stdprome.HistogramOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "health_check_duration_ms",
		Help:      "Duration of node health checks in milliseconds",
		Buckets:   []float64{10, 50, 100, 500, 1000, 3000, 5000, 10000, 30000},
	}, []string{"chain_id", "node_key", "status"})
)

// IncrNodeHealthCheckTotal increments the node health check counter
func IncrNodeHealthCheckTotal(chainID, nodeKey, status string) {
	nodeHealthCheckTotal.With("chain_id", chainID, "node_key", nodeKey, "status", status).Add(1)
}

// ObserveNodeHealthCheckDuration observes the duration of a node health check
func ObserveNodeHealthCheckDuration(chainID, nodeKey, status string, duration time.Duration) {
	nodeHealthCheckDuration.With("chain_id", chainID, "node_key", nodeKey, "status", status).Observe(float64(duration.Milliseconds()))
}
