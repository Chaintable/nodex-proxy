package lb

import (
	"bytes"
	"context"
	"strings"
	"syscall"
	"testing"

	"github.com/Chaintable/nodex-proxy/discovery"
	"github.com/Chaintable/nodex-proxy/internal/benchfixture"
	"github.com/Chaintable/nodex-proxy/lb/lbnode"
	"github.com/ethereum/go-ethereum/common"
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

// Unlike the single-pool benchmark, these scenarios must resolve block context
// to choose between state/archive or consult the archive block-hash cache.
func BenchmarkServeHTTPBlockContextCPU(b *testing.B) {
	requests, responses := benchfixture.Requests(), benchfixture.Responses()
	for _, name := range []string{"splitPools", "archiveHashHit", "batchSplitPools"} {
		b.Run(name, func(b *testing.B) {
			body := requests["multicall"]
			hash := common.HexToHash("0x" + strings.Repeat("ab", 32))
			if name == "archiveHashHit" {
				body = bytes.Replace(body, []byte(`"block_id":"0x1234"`), []byte(`"block_id":"`+hash.Hex()+`"`), 1)
			} else if name == "batchSplitPools" {
				body = requests["batch"]
			}
			setup := func(lb *LoadBalancer) {
				nodes, _ := lb.NodeSelector.GetStateNodes("1")
				if len(nodes) != 1 {
					b.Fatal("missing state node")
				}
				node, err := lbnode.New("bench-archive", nodes[0].IP(), nodes[0].Port(), 100, discovery.NodeTypeArchive)
				if err != nil {
					b.Fatal(err)
				}
				node.SetState(1)
				if err := lb.NodeSelector.UpsertNode(context.Background(), "1", discovery.NodeTypeArchive, node); err != nil {
					b.Fatal(err)
				}
				if name == "archiveHashHit" {
					lb.archiveHashCache = newArchiveHashCache(1024)
					lb.archiveHashCache.Add("1", hash)
				}
			}
			var before, after syscall.Rusage
			if err := syscall.Getrusage(syscall.RUSAGE_SELF, &before); err != nil {
				b.Fatal(err)
			}
			benchServeWithSetup(b, body, responses["multicall256KiB"], setup)
			b.StopTimer()
			if err := syscall.Getrusage(syscall.RUSAGE_SELF, &after); err != nil {
				b.Fatal(err)
			}
			cpu := after.Utime.Nano() + after.Stime.Nano() - before.Utime.Nano() - before.Stime.Nano()
			b.ReportMetric(float64(cpu)/float64(b.N), "cpu-ns/op")
		})
	}
}
