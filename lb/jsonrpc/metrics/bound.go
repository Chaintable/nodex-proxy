package metrics

import (
	"strconv"
	"sync"
	"sync/atomic"
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
// metrics, directly through the vectors, without adding permanent cache entries.
const (
	maxMethodMetricEntries    = 16 * 1024
	maxMethodMetricLabelBytes = 1024
)

type methodMetricCache struct {
	entries sync.Map   // methodMetricKey -> *methodHandles
	mu      sync.Mutex // serializes admission, never taken on cache hits
	size    int
	limit   int
	// Entries are never evicted, so once full the cache stays full. Misses
	// then skip mu: random labels must not serialize requests on one lock.
	full atomic.Bool
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

// methodHandles binds labels lazily. Merely caching handles must not create
// unused zero-valued time series (e.g. calls_finished for a failed request).
type methodHandles struct {
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

// MethodMetrics records the per-method metrics of one request. Cached label
// tuples reuse bound handles; the rest record through the vectors directly,
// which costs no more than not having the cache at all.
type MethodMetrics struct {
	key     methodMetricKey
	handles *methodHandles // nil when the tuple is not cached
}

func (m CommonLabelMetrics) ForMethod(method jsonrpc.RPCMethod) MethodMetrics {
	key := methodMetricKey{CommonLabelMetrics: m, method: method}
	return MethodMetrics{key: key, handles: methodMetrics.handlesFor(key)}
}

// handlesFor stores key as is: label values are owned strings by contract
// (see CommonLabelMetrics), so the cache cannot pin request buffers.
func (cache *methodMetricCache) handlesFor(key methodMetricKey) *methodHandles {
	if len(key.host)+len(key.target)+len(key.chainID)+len(key.chainVersion)+len(key.method) > maxMethodMetricLabelBytes {
		return nil
	}
	if existing, ok := cache.entries.Load(key); ok {
		return existing.(*methodHandles)
	}
	if cache.full.Load() {
		return nil
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	// A racing caller may have populated the tuple while we waited.
	if existing, ok := cache.entries.Load(key); ok {
		return existing.(*methodHandles)
	}
	if cache.size >= cache.limit {
		cache.full.Store(true)
		return nil
	}
	handles := &methodHandles{}
	cache.entries.Store(key, handles)
	cache.size++
	return handles
}

// Label values must follow each vector's declaration order.

func (k *methodMetricKey) total() prometheus.Counter {
	return totalJRPCRequest.WithLabelValues(k.host, k.target, k.chainID, k.chainVersion)
}

func (k *methodMetricKey) finished() prometheus.Counter {
	return callsFinished.WithLabelValues(k.host, k.target, k.chainID, k.chainVersion, string(k.method))
}

func (k *methodMetricKey) cacheHits() prometheus.Counter {
	return callsCacheHits.WithLabelValues(k.host, k.target, k.chainID, k.chainVersion, string(k.method))
}

func (k *methodMetricKey) batchFinished() prometheus.Counter {
	return batchCallsFinished.WithLabelValues(k.host, k.target, k.chainID, k.chainVersion)
}

func (k *methodMetricKey) callsDuration() prometheus.Observer {
	return callsTime.WithLabelValues(k.host, k.target, k.chainID, k.chainVersion, string(k.method))
}

func (k *methodMetricKey) batchDuration() prometheus.Observer {
	return batchCallsTime.WithLabelValues(k.host, k.target, k.chainID, k.chainVersion)
}

func (k *methodMetricKey) requestSize() prometheus.Observer {
	return requestPayloadSizes.WithLabelValues(k.host, k.target, k.chainID, k.chainVersion, string(k.method))
}

func (k *methodMetricKey) responseSize() prometheus.Observer {
	return responsePayloadSizes.WithLabelValues(k.host, k.target, k.chainID, k.chainVersion, string(k.method))
}

func (k *methodMetricKey) httpStatus(statusCode int) prometheus.Counter {
	method := string(k.method)
	if method == "" {
		method = "batch"
	}
	return httpStatusCode.WithLabelValues(k.host, k.target, k.chainID, k.chainVersion, method, strconv.Itoa(statusCode))
}

func (m *MethodMetrics) IncrTotalJRPCRequest() {
	if m.handles == nil {
		m.key.total().Inc()
		return
	}
	m.handles.total.get(m.key.total).Inc()
}

func (m *MethodMetrics) IncrCallsFinished() {
	if m.handles == nil {
		m.key.finished().Inc()
		return
	}
	m.handles.finished.get(m.key.finished).Inc()
}

func (m *MethodMetrics) IncrCallsCacheHits() {
	if m.handles == nil {
		m.key.cacheHits().Inc()
		return
	}
	m.handles.cacheHits.get(m.key.cacheHits).Inc()
}

func (m *MethodMetrics) IncrBatchCallsFinished() {
	if m.handles == nil {
		m.key.batchFinished().Inc()
		return
	}
	m.handles.batchFinished.get(m.key.batchFinished).Inc()
}

func (m *MethodMetrics) ObCallsTime(duration time.Duration) {
	ms := float64(duration.Milliseconds())
	if m.handles == nil {
		m.key.callsDuration().Observe(ms)
		return
	}
	m.handles.callsDuration.get(m.key.callsDuration).Observe(ms)
}

func (m *MethodMetrics) ObBatchCallsTime(duration time.Duration) {
	ms := float64(duration.Milliseconds())
	if m.handles == nil {
		m.key.batchDuration().Observe(ms)
		return
	}
	m.handles.batchDuration.get(m.key.batchDuration).Observe(ms)
}

func (m *MethodMetrics) ObRequestPayloadSizes(size int) {
	if m.handles == nil {
		m.key.requestSize().Observe(float64(size))
		return
	}
	m.handles.requestSize.get(m.key.requestSize).Observe(float64(size))
}

func (m *MethodMetrics) ObResponsePayloadSizes(size int) {
	if m.handles == nil {
		m.key.responseSize().Observe(float64(size))
		return
	}
	m.handles.responseSize.get(m.key.responseSize).Observe(float64(size))
}

func (m *MethodMetrics) IncrHTTPStatusCode(statusCode int) {
	if m.handles == nil {
		m.key.httpStatus(statusCode).Inc()
		return
	}
	if counter, ok := m.handles.httpStatuses.Load(statusCode); ok {
		counter.(prometheus.Counter).Inc()
		return
	}
	bound, _ := m.handles.httpStatuses.LoadOrStore(statusCode, m.key.httpStatus(statusCode))
	bound.(prometheus.Counter).Inc()
}
