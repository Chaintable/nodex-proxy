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
	discardedRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "jrpcx",
		Subsystem: "usage",
		Name:      "discarded_requests_total",
		Help:      "Number of request usage samples discarded before aggregation.",
	}, []string{"reason"})
)

func init() {
	prometheus.MustRegister(flushTotal, recordsTotal, discardedRequestsTotal)
}
