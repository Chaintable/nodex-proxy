package metrics

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Chaintable/nodex-proxy/jsonrpc"
	"github.com/prometheus/client_golang/prometheus"
)

type methodMetricKey struct {
	CommonLabelMetrics
	method jsonrpc.RPCMethod
}

// Like the underlying Prometheus vectors, handles live for the process lifetime.
// One lookup per postprocessor replaces repeated hashing of the common labels.
// Failure labels (including node addresses) and source-dapp labels stay on the
// existing vectors and do not add dimensions to this cache.
var methodMetrics sync.Map // methodMetricKey -> *MethodMetrics

type metricHandle[T any] struct {
	once   sync.Once
	metric T
}

func (h *metricHandle[T]) get(bind func() T) T {
	h.once.Do(func() { h.metric = bind() })
	return h.metric
}

// MethodMetrics binds labels lazily. Merely obtaining a handle must not create
// unused zero-valued time series (e.g. calls_finished for a failed request).
type MethodMetrics struct {
	key methodMetricKey

	total         metricHandle[prometheus.Counter]
	finished      metricHandle[prometheus.Counter]
	cacheHits     metricHandle[prometheus.Counter]
	batchFinished metricHandle[prometheus.Counter]
	callsDuration metricHandle[prometheus.Observer]
	batchDuration metricHandle[prometheus.Observer]
	requestSize   metricHandle[prometheus.Observer]
	responseSize  metricHandle[prometheus.Observer]
	httpStatuses  sync.Map // int -> prometheus.Counter
}

func (m CommonLabelMetrics) ForMethod(method jsonrpc.RPCMethod) *MethodMetrics {
	key := methodMetricKey{CommonLabelMetrics: m, method: method}
	if existing, ok := methodMetrics.Load(key); ok {
		return existing.(*MethodMetrics)
	}
	// A decoded method can be a substring of a large RPC body. Cache only
	// owned label bytes, so the additional cache cannot pin request buffers.
	key = methodMetricKey{
		CommonLabelMetrics: CommonLabelMetrics{
			host: strings.Clone(m.host), target: strings.Clone(m.target),
			chainID: strings.Clone(m.chainID), chainVersion: strings.Clone(m.chainVersion),
		},
		method: jsonrpc.RPCMethod(strings.Clone(string(method))),
	}
	bound, _ := methodMetrics.LoadOrStore(key, &MethodMetrics{key: key})
	return bound.(*MethodMetrics)
}

func (m *MethodMetrics) IncrTotalJRPCRequest() {
	m.total.get(func() prometheus.Counter {
		k := m.key
		return totalJRPCRequest.WithLabelValues(k.host, k.target, k.chainID, k.chainVersion)
	}).Inc()
}

func (m *MethodMetrics) IncrCallsFinished() {
	m.finished.get(func() prometheus.Counter {
		k := m.key
		return callsFinished.WithLabelValues(k.host, k.target, k.chainID, k.chainVersion, string(k.method))
	}).Inc()
}

func (m *MethodMetrics) IncrCallsCacheHits() {
	m.cacheHits.get(func() prometheus.Counter {
		k := m.key
		return callsCacheHits.WithLabelValues(k.host, k.target, k.chainID, k.chainVersion, string(k.method))
	}).Inc()
}

func (m *MethodMetrics) IncrBatchCallsFinished() {
	m.batchFinished.get(func() prometheus.Counter {
		k := m.key
		return batchCallsFinished.WithLabelValues(k.host, k.target, k.chainID, k.chainVersion)
	}).Inc()
}

func (m *MethodMetrics) ObCallsTime(duration time.Duration) {
	m.callsDuration.get(func() prometheus.Observer {
		k := m.key
		return callsTime.WithLabelValues(k.host, k.target, k.chainID, k.chainVersion, string(k.method))
	}).Observe(float64(duration.Milliseconds()))
}

func (m *MethodMetrics) ObBatchCallsTime(duration time.Duration) {
	m.batchDuration.get(func() prometheus.Observer {
		k := m.key
		return batchCallsTime.WithLabelValues(k.host, k.target, k.chainID, k.chainVersion)
	}).Observe(float64(duration.Milliseconds()))
}

func (m *MethodMetrics) ObRequestPayloadSizes(size int) {
	m.requestSize.get(func() prometheus.Observer {
		k := m.key
		return requestPayloadSizes.WithLabelValues(k.host, k.target, k.chainID, k.chainVersion, string(k.method))
	}).Observe(float64(size))
}

func (m *MethodMetrics) ObResponsePayloadSizes(size int) {
	m.responseSize.get(func() prometheus.Observer {
		k := m.key
		return responsePayloadSizes.WithLabelValues(k.host, k.target, k.chainID, k.chainVersion, string(k.method))
	}).Observe(float64(size))
}

func (m *MethodMetrics) IncrHTTPStatusCode(statusCode int) {
	if counter, ok := m.httpStatuses.Load(statusCode); ok {
		counter.(prometheus.Counter).Inc()
		return
	}
	k := m.key
	method := string(k.method)
	if method == "" {
		method = "batch"
	}
	counter := httpStatusCode.WithLabelValues(k.host, k.target, k.chainID, k.chainVersion, method, strconv.Itoa(statusCode))
	bound, _ := m.httpStatuses.LoadOrStore(statusCode, counter)
	bound.(prometheus.Counter).Inc()
}
