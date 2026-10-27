package metrics

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Chaintable/nodex-proxy/jsonrpc"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

var metricTestSequence atomic.Uint64

func metricSnapshot(t *testing.T, registry *prometheus.Registry, host string) []*dto.MetricFamily {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	var result []*dto.MetricFamily
	for _, family := range families {
		filtered := proto.Clone(family).(*dto.MetricFamily)
		filtered.Metric = nil
		for _, metric := range family.Metric {
			copy := proto.Clone(metric).(*dto.Metric)
			matched := false
			for _, label := range copy.Label {
				if label.GetName() == "host" && label.GetValue() == host {
					label.Value = proto.String("normalized-host")
					matched = true
				}
			}
			if !matched {
				continue
			}
			// Creation time varies between the two equivalent label sets.
			if copy.Counter != nil {
				copy.Counter.CreatedTimestamp = nil
			}
			if copy.Histogram != nil {
				copy.Histogram.CreatedTimestamp = nil
			}
			filtered.Metric = append(filtered.Metric, copy)
		}
		if len(filtered.Metric) > 0 {
			result = append(result, filtered)
		}
	}
	return result
}

// recordDirect writes the expected series through the vectors with explicit
// label values in declaration order, independent of the bound-handle code.
func recordDirect(host string, method jsonrpc.RPCMethod) {
	common := []string{host, "native", "1", "v1"}
	labels := func(extra ...string) []string { return append(append([]string(nil), common...), extra...) }
	totalJRPCRequest.WithLabelValues(common...).Inc()
	statusMethod := string(method)
	if statusMethod == "" {
		statusMethod = "batch"
	}
	for _, status := range []int{200, 200, 408, 502, 504} {
		httpStatusCode.WithLabelValues(labels(statusMethod, strconv.Itoa(status))...).Inc()
	}
	if method == "" {
		batchCallsFinished.WithLabelValues(common...).Inc()
		batchCallsTime.WithLabelValues(common...).Observe(12)
		return
	}
	callsFinished.WithLabelValues(labels(string(method))...).Inc()
	callsTime.WithLabelValues(labels(string(method))...).Observe(12)
	requestPayloadSizes.WithLabelValues(labels(string(method))...).Observe(40000)
	responsePayloadSizes.WithLabelValues(labels(string(method))...).Observe(262144)
}

func TestBoundMetricsPreserveExposition(t *testing.T) {
	registry := prometheus.NewRegistry()
	registry.MustRegister(totalJRPCRequest, callsFinished, callsFailed, batchCallsFinished, callsTime, batchCallsTime, requestPayloadSizes, responsePayloadSizes, httpStatusCode)
	sequence := metricTestSequence.Add(1)
	for _, method := range []jsonrpc.RPCMethod{"contractMultiCall", "eth_getBalance", "", "batch"} {
		t.Run(string(method), func(t *testing.T) {
			directHost := fmt.Sprintf("direct-%d-%s", sequence, method)
			recordDirect(directHost, method)
			expected := metricSnapshot(t, registry, directHost)
			for _, cached := range []bool{true, false} {
				host := fmt.Sprintf("bound-%t-%d-%s", cached, sequence, method)
				bound := NewCommonLabelMetrics(types.ProcessorHost(host), "native", "1", "v1").ForMethod(method)
				require.NotNil(t, bound.handles)
				if !cached {
					bound.handles = nil // the path taken when the cache is full
				}
				require.Empty(t, metricSnapshot(t, registry, host), "binding must not create unused time series")
				bound.IncrTotalJRPCRequest()
				for _, status := range []int{200, 200, 408, 502, 504} {
					bound.IncrHTTPStatusCode(status)
				}
				if method == "" {
					bound.IncrBatchCallsFinished()
					bound.ObBatchCallsTime(12 * time.Millisecond)
				} else {
					bound.IncrCallsFinished()
					bound.ObCallsTime(12 * time.Millisecond)
					bound.ObRequestPayloadSizes(40000)
					bound.ObResponsePayloadSizes(262144)
				}
				actual := metricSnapshot(t, registry, host)
				require.Len(t, actual, len(expected))
				for i := range expected {
					require.True(t, proto.Equal(expected[i], actual[i]), "cached=%t: want %s; got %s", cached, expected[i], actual[i])
				}
			}
		})
	}
}

func TestBoundMetricsConcurrentColdStart(t *testing.T) {
	const workers = 32
	host := types.ProcessorHost(fmt.Sprintf("concurrent-test-%d", metricTestSequence.Add(1)))
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			// Some tuples are shared; others differ only in chain/version/target.
			m := NewCommonLabelMetrics(host, types.ProcessorTarget(fmt.Sprint(i%2)), fmt.Sprint(i%4), fmt.Sprint(i%8))
			for range 100 {
				bound := m.ForMethod("contractMultiCall")
				bound.IncrTotalJRPCRequest()
				bound.IncrHTTPStatusCode(200)
				bound.IncrCallsFinished()
				bound.ObCallsTime(time.Millisecond)
				bound.ObRequestPayloadSizes(1024)
				bound.ObResponsePayloadSizes(2048)
			}
		})
	}
	wg.Wait()
	registry := prometheus.NewRegistry()
	registry.MustRegister(totalJRPCRequest, callsFinished, httpStatusCode, callsTime, requestPayloadSizes, responsePayloadSizes)
	families := metricSnapshot(t, registry, string(host))
	require.Len(t, families, 6)
	for _, family := range families {
		require.Len(t, family.Metric, 8)
		var total float64
		for _, metric := range family.Metric {
			if metric.Counter != nil {
				total += metric.Counter.GetValue()
			} else {
				total += float64(metric.Histogram.GetSampleCount())
			}
		}
		require.Equal(t, float64(workers*100), total, family.GetName())
	}
}

func TestMethodMetricCacheAdmissionLimit(t *testing.T) {
	cache := methodMetricCache{limit: 2}
	common := NewCommonLabelMetrics(types.ProcessorHost(fmt.Sprintf("limited-%d", metricTestSequence.Add(1))), "native", "1", "v1")
	key := func(method string) methodMetricKey {
		return methodMetricKey{CommonLabelMetrics: common, method: jsonrpc.RPCMethod(method)}
	}
	hot := cache.handlesFor(key("hot"))
	require.NotNil(t, hot)
	require.Same(t, hot, cache.handlesFor(key("hot")))
	require.NotNil(t, cache.handlesFor(key("second")))
	// Misses after the cap create no entry; metrics still record directly.
	require.Nil(t, cache.handlesFor(key("overflow")))
	overflow := MethodMetrics{key: key("overflow")}
	overflow.IncrCallsFinished()
	overflow.IncrCallsFinished()
	metric := &dto.Metric{}
	require.NoError(t, callsFinished.WithLabelValues(common.host, common.target, common.chainID, common.chainVersion, "overflow").Write(metric))
	require.Equal(t, float64(2), metric.Counter.GetValue())
	require.Equal(t, 2, cache.size)
	require.Same(t, hot, cache.handlesFor(key("hot")), "hitting the cap must not evict hot handles")

	// Once full, misses must not wait for the admission lock.
	cache.mu.Lock()
	defer cache.mu.Unlock()
	done := make(chan *methodHandles, 1)
	go func() { done <- cache.handlesFor(key("late")) }()
	select {
	case late := <-done:
		require.Nil(t, late)
	case <-time.After(5 * time.Second):
		t.Fatal("full cache miss blocked on the admission lock")
	}
	require.Same(t, hot, cache.handlesFor(key("hot")))
}

func TestMethodMetricCacheConcurrentAdmission(t *testing.T) {
	cache := methodMetricCache{limit: 4}
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Go(func() {
			cache.handlesFor(methodMetricKey{method: jsonrpc.RPCMethod(fmt.Sprint(i))})
		})
	}
	wg.Wait()
	entries := 0
	cache.entries.Range(func(_, _ any) bool { entries++; return true })
	require.Equal(t, 4, entries)
	require.Equal(t, 4, cache.size)
}

func TestMethodMetricCacheRejectsOversizedKeys(t *testing.T) {
	cache := methodMetricCache{limit: 2}
	for field := range 5 {
		key := methodMetricKey{}
		long := strings.Repeat("x", maxMethodMetricLabelBytes+1)
		switch field {
		case 0:
			key.host = long
		case 1:
			key.target = long
		case 2:
			key.chainID = long
		case 3:
			key.chainVersion = long
		case 4:
			key.method = jsonrpc.RPCMethod(long)
		}
		require.Nil(t, cache.handlesFor(key))
	}
	require.Zero(t, cache.size)
}
