package usage

import "github.com/prometheus/client_golang/prometheus"

var (
	flushTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "jrpcx",
		Subsystem: "usage",
		Name:      "flush_total",
		Help:      "Number of Kafka usage flushes by result.",
	}, []string{"status"})
	recordsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "jrpcx",
		Subsystem: "usage",
		Name:      "records_total",
		Help:      "Number of aggregated usage records by delivery result.",
	}, []string{"status"})
	aggregationKeys = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "jrpcx",
		Subsystem: "usage",
		Name:      "aggregation_keys",
		Help:      "Current number of client ID aggregation keys held in memory, including batches being written.",
	})
)

func init() {
	prometheus.MustRegister(flushTotal, recordsTotal, aggregationKeys)
}
