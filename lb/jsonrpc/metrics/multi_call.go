package metrics

import (
	"github.com/go-kit/kit/metrics/prometheus"
	stdprome "github.com/prometheus/client_golang/prometheus"
)

var (
	lvsLabelNames  = newLabelNames(commonLabelNames, "cache", "biz", "scenario")
	ilvsLabelNames = newLabelNames(lvsLabelNames, "code")
)

func (m CommonLabelMetrics) ObMultiCallInnerCallDuration(ilvs []string, timeCost float64) {
	multiCallInnerCallDuration.With(m.labelValues...).With(ilvs...).Observe(timeCost)
}

func (m CommonLabelMetrics) ObMultiCallInnerCallGasUsed(ilvs []string, gasUsed int) {
	multiCallInnerCallGasUsed.With(m.labelValues...).With(ilvs...).Observe(float64(gasUsed))
}

func (m CommonLabelMetrics) ObMultiCallConcurrency(lvs []string, count int) {
	multiCallConcurrency.With(m.labelValues...).With(lvs...).Observe(float64(count))
}

func (m CommonLabelMetrics) MultiCallCacheHit(lvs []string, count float64) {
	multiCallCacheHit.With(m.labelValues...).With(lvs...).Add(count)
}

var (
	multiCallConcurrency = prometheus.NewHistogramFrom(stdprome.HistogramOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "calls_concurrency",
		Help:      "eth_multiCall concurrency, count up to total eth_multiCalls and sum up to total inner calls",
		Buckets:   []float64{1, 2, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50}, // node supports up to 50 concurrent calls
	}, lvsLabelNames)
	multiCallCacheHit = prometheus.NewCounterFrom(stdprome.CounterOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "calls_cachehit",
		Help:      "eth_multiCall inner calls cache hit",
	}, lvsLabelNames)
	multiCallInnerCallDuration = prometheus.NewHistogramFrom(stdprome.HistogramOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "calls_duration",
		Help:      "inner calls duration stats. sum up to total time cost, flip the cache label to calculate time saved",
		Buckets:   []float64{0.0001, 0.0002, 0.0005, 0.0008, 0.001, 0.002, 0.003, 0.005, 0.01, 0.02, 0.03, 0.05}, // a single evm call is single-digit millisecond
	}, ilvsLabelNames)
	multiCallInnerCallGasUsed = prometheus.NewHistogramFrom(stdprome.HistogramOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "calls_gasused",
		Help:      "inner calls gasused stats. sum up to total gasued, flip the cache label to calculate gas saved",
		Buckets:   []float64{22000, 25000, 28000, 30000, 32000, 35000, 38000, 40000, 45000, 50000, 60000, 80000},
	}, ilvsLabelNames)
)
