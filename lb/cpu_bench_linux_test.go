package lb

import (
	"syscall"
	"testing"

	"github.com/Chaintable/nodex-proxy/internal/benchfixture"
)

// Wall-clock HTTP benchmarks on a shared host are sensitive to unrelated
// workloads. Also report process user+system CPU per completed request. This
// includes the local mock upstream and amortized setup; it is not a prediction
// of production ARM latency or absolute proxy CPU cost.
func BenchmarkServeHTTPCPU(b *testing.B) {
	requests, responses := benchfixture.Requests(), benchfixture.Responses()
	for _, name := range []string{"small", "multicall"} {
		b.Run(name, func(b *testing.B) {
			response := responses["small"]
			if name == "multicall" {
				response = responses["multicall256KiB"]
			}
			var before, after syscall.Rusage
			if err := syscall.Getrusage(syscall.RUSAGE_SELF, &before); err != nil {
				b.Fatal(err)
			}
			benchServe(b, requests[name], response)
			b.StopTimer()
			if err := syscall.Getrusage(syscall.RUSAGE_SELF, &after); err != nil {
				b.Fatal(err)
			}
			cpu := after.Utime.Nano() + after.Stime.Nano() - before.Utime.Nano() - before.Stime.Nano()
			b.ReportMetric(float64(cpu)/float64(b.N), "cpu-ns/op")
		})
	}
}
