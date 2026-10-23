package metrics

import (
	"fmt"
	"strings"
	"testing"
	"unsafe"

	"github.com/Chaintable/nodex-proxy/jsonrpc"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestDirectMetricMethodLabelsOwnTheirBytes(t *testing.T) {
	body := strings.Repeat("x", 1<<20) + "contractMultiCall"
	method := jsonrpc.RPCMethod(body[len(body)-len("contractMultiCall"):])
	host := types.ProcessorHost(fmt.Sprintf("owned-label-%d", metricTestSequence.Add(1)))
	m := NewCommonLabelMetrics(host, "native", "1", "v1")
	m.IncrCallsStarted(method, "dapp")
	m.IncrCallsFailed(jsonrpc.InvalidResponseCode, method, true, "upstream", "node")
	counters := []prometheus.Counter{
		callsStarted.WithLabelValues(string(host), "native", "1", "v1", string(method), "dapp"),
		callsFailed.WithLabelValues(string(host), "native", "1", "v1", string(method), fmt.Sprint(jsonrpc.InvalidResponseCode), "true", "upstream", "node"),
	}
	for _, counter := range counters {
		metric := &dto.Metric{}
		require.NoError(t, counter.Write(metric))
		require.Equal(t, float64(1), metric.Counter.GetValue())
		for _, label := range metric.Label {
			if label.GetName() == "method" {
				require.Equal(t, string(method), label.GetValue())
				require.NotEqual(t, uintptr(unsafe.Pointer(unsafe.StringData(string(method)))), uintptr(unsafe.Pointer(unsafe.StringData(label.GetValue()))), "time series must not keep the request body backing the method")
			}
		}
	}
}
