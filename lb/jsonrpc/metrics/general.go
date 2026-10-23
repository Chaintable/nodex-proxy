package metrics

import (
	"strconv"
	"time"

	"github.com/Chaintable/nodex-proxy/jsonrpc"
	"github.com/Chaintable/nodex-proxy/types"
	stdprome "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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

// CommonLabelMetrics carries the per-request common label values. Metrics are
// recorded through prometheus *Vec.WithLabelValues directly: label values must
// be passed in the exact order the vector was declared with (common labels
// first, extra labels after), which avoids the per-call label-map allocation
// and sort that a name/value API needs. A new time series retains its label
// strings, so callers must pass owned strings, never substrings of request
// buffers. Per-method metrics go through ForMethod (bound.go).
type CommonLabelMetrics struct {
	host         string
	target       string
	chainID      string
	chainVersion string
}

func NewCommonLabelMetrics(host types.ProcessorHost, target types.ProcessorTarget, chainID, chainVersion string) CommonLabelMetrics {
	return CommonLabelMetrics{
		host:         string(host),
		target:       string(target),
		chainID:      chainID,
		chainVersion: chainVersion,
	}
}

func (m CommonLabelMetrics) IncrCallsStarted(method jsonrpc.RPCMethod, sourcedapp string) {
	callsStarted.WithLabelValues(m.host, m.target, m.chainID, m.chainVersion, string(method), sourcedapp).Inc()
}

func (m CommonLabelMetrics) IncrCallsFailed(code jsonrpc.ErrorCode, method jsonrpc.RPCMethod, upstreamRelated bool, reason string, nodeAddr string) {
	callsFailed.WithLabelValues(
		m.host, m.target, m.chainID, m.chainVersion,
		string(method),
		strconv.FormatInt(int64(code), 10),
		strconv.FormatBool(upstreamRelated),
		reason,
		nodeAddr,
	).Inc()
}

func (m CommonLabelMetrics) IncrInternalFailedRequest() {
	internalFailedRequest.WithLabelValues(m.host, m.target, m.chainID, m.chainVersion).Inc()
}

var (
	callsStarted = promauto.NewCounterVec(stdprome.CounterOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "calls_started",
		Help:      "Number of received RPC calls (unique un-batched requests)",
	}, methodSourceDappCommonLabelNames)
	callsFinished = promauto.NewCounterVec(stdprome.CounterOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "calls_finished",
		Help:      "Number of processed RPC calls (unique un-batched requests)",
	}, methodCommonLabelNames)
	callsCacheHits = promauto.NewCounterVec(stdprome.CounterOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "calls_cache_hits",
		Help:      "Number of hit the cache RPC calls (unique un-batched requests)",
	}, methodCommonLabelNames)
	callsFailed = promauto.NewCounterVec(stdprome.CounterOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "calls_failed",
		Help:      "Number of failure RPC calls (unique un-batched requests)",
	}, newLabelNames(methodCommonLabelNames, "status_code", "upstream_related", "reason", "node_addr"))
	batchCallsFinished = promauto.NewCounterVec(stdprome.CounterOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "batch_calls_finished",
		Help:      "Number of processed RPC batch calls",
	}, commonLabelNames)
	callsTime = promauto.NewHistogramVec(stdprome.HistogramOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "calls_time",
		Help:      "Request duration in milliseconds of RPC calls",
		Buckets:   []float64{1, 5, 10, 25, 50, 100, 300, 500, 1000, 3000, 5000, 10000},
	}, methodCommonLabelNames)
	batchCallsTime = promauto.NewHistogramVec(stdprome.HistogramOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "batch_calls_time",
		Help:      "Request duration in milliseconds of RPC batch calls",
		Buckets:   []float64{1, 5, 10, 25, 50, 100, 300, 500, 1000, 3000, 5000, 10000},
	}, commonLabelNames)
	totalJRPCRequest = promauto.NewCounterVec(stdprome.CounterOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "request",
		Help:      "Total request count",
	}, commonLabelNames)
	requestPayloadSizes = promauto.NewHistogramVec(stdprome.HistogramOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "request_payload_sizes",
		Help:      "Histogram of RPC request payload sizes",
		Buckets:   []float64{10, 50, 100, 500, 1_000, 5_000, 10_000, 100_000, 1_000_000},
	}, methodCommonLabelNames)
	responsePayloadSizes = promauto.NewHistogramVec(stdprome.HistogramOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "response_payload_sizes",
		Help:      "Histogram of RPC response payload sizes",
		Buckets:   []float64{10, 50, 100, 500, 1_000, 5_000, 10_000, 100_000, 1_000_000},
	}, methodCommonLabelNames)
	internalFailedRequest = promauto.NewCounterVec(stdprome.CounterOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "internal_failed_request",
		Help:      "Number of failed internal requests",
	}, commonLabelNames)
	httpStatusCode = promauto.NewCounterVec(stdprome.CounterOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "http_status_code",
		Help:      "HTTP status code counter",
	}, newLabelNames(commonLabelNames, "method", "status_code"))
	nodeHealthCheckTotal = promauto.NewCounterVec(stdprome.CounterOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "health_check_total",
		Help:      "Total number of node health checks performed",
	}, []string{"chain_id", "node_key", "status"})
	archiveHashCacheTotal = promauto.NewCounterVec(stdprome.CounterOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "archive_hash_cache_total",
		Help:      "Archive block-hash cache events: hit routes straight to archive, add records a hash after a state miss",
	}, []string{"chain_id", "event"})
	nodeHealthCheckDuration = promauto.NewHistogramVec(stdprome.HistogramOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "health_check_duration_ms",
		Help:      "Duration of node health checks in milliseconds",
		Buckets:   []float64{10, 50, 100, 500, 1000, 3000, 5000, 10000, 30000},
	}, []string{"chain_id", "node_key", "status"})
)

// IncrNodeHealthCheckTotal increments the node health check counter
func IncrNodeHealthCheckTotal(chainID, nodeKey, status string) {
	nodeHealthCheckTotal.WithLabelValues(chainID, nodeKey, status).Inc()
}

// ObserveNodeHealthCheckDuration observes the duration of a node health check
func ObserveNodeHealthCheckDuration(chainID, nodeKey, status string, duration time.Duration) {
	nodeHealthCheckDuration.WithLabelValues(chainID, nodeKey, status).Observe(float64(duration.Milliseconds()))
}

// IncrArchiveHashCache counts archive block-hash cache events (hit/add).
func IncrArchiveHashCache(chainID, event string) {
	archiveHashCacheTotal.WithLabelValues(chainID, event).Inc()
}
