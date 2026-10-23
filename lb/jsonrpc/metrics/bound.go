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

// Bound only the additional handle cache; Prometheus keeps its existing label
// sets and accounting. Unusual labels and entries beyond the cap still record
// metrics through request-local handles, without adding permanent cache entries.
const (
	maxMethodMetricEntries    = 16 * 1024
	maxMethodMetricLabelBytes = 1024
)

type methodMetricCache struct {
	entries sync.Map   // methodMetricKey -> *MethodMetrics
	mu      sync.Mutex // serializes admission, never taken on cache hits
	size    int
	limit   int
}

var methodMetrics = methodMetricCache{limit: maxMethodMetricEntries}

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
	return methodMetrics.forMethod(methodMetricKey{CommonLabelMetrics: m, method: method})
}

func (cache *methodMetricCache) forMethod(key methodMetricKey) *MethodMetrics {
	if len(key.host)+len(key.target)+len(key.chainID)+len(key.chainVersion)+len(key.method) > maxMethodMetricLabelBytes {
		return newMethodMetrics(key)
	}
	if existing, ok := cache.entries.Load(key); ok {
		return existing.(*MethodMetrics)
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	// A racing caller may have populated the tuple while we waited.
	if existing, ok := cache.entries.Load(key); ok {
		return existing.(*MethodMetrics)
	}
	bound := newMethodMetrics(key)
	if cache.size < cache.limit {
		cache.entries.Store(bound.key, bound)
		cache.size++
	}
	return bound
}

func newMethodMetrics(key methodMetricKey) *MethodMetrics {
	// Both cached and request-local handles must own their labels: a Prometheus
	// vector can retain the supplied strings when creating a time series.
	return &MethodMetrics{key: methodMetricKey{
		CommonLabelMetrics: CommonLabelMetrics{
			host: strings.Clone(key.host), target: strings.Clone(key.target),
			chainID: strings.Clone(key.chainID), chainVersion: strings.Clone(key.chainVersion),
		},
		method: jsonrpc.RPCMethod(strings.Clone(string(key.method))),
	}}
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
