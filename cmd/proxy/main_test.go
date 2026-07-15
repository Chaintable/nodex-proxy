package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Chaintable/nodex-proxy/types"
	"github.com/Chaintable/nodex-proxy/usage"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/route/param"
	"github.com/stretchr/testify/require"
)

type fakeRPCRequestHandler struct {
	chainID string
}

func (h *fakeRPCRequestHandler) ServeHTTP(_ context.Context, _ *app.RequestContext, chainID string) {
	h.chainID = chainID
	time.Sleep(2 * time.Millisecond)
}

type capturingUsageProducer struct {
	mu      sync.Mutex
	records []usage.Record
}

func (p *capturingUsageProducer) Write(_ context.Context, records []usage.Record) error {
	p.mu.Lock()
	p.records = append(p.records, records...)
	p.mu.Unlock()
	return nil
}

func (p *capturingUsageProducer) Close() error { return nil }

func (p *capturingUsageProducer) snapshot() []usage.Record {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]usage.Record(nil), p.records...)
}

func TestRPCRequestHandlerCollectsHeaderChainAndDuration(t *testing.T) {
	tests := []struct {
		name         string
		clientID     string
		expectedID   string
		pathChainID  string
		expectedBase int64
	}{
		{
			name:         "missing client id",
			expectedID:   usage.UnknownClientID,
			pathChainID:  "0x1-version-id",
			expectedBase: 1,
		},
		{
			name:         "provided client id",
			clientID:     "instance:123",
			expectedID:   "instance:123",
			pathChainID:  "56",
			expectedBase: 56,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			producer := &capturingUsageProducer{}
			collector := usage.NewCollector(producer)
			rpcHandler := &fakeRPCRequestHandler{}
			handler := newRPCRequestHandler(rpcHandler, collector)
			requestContext := app.NewContext(1)
			requestContext.Params = param.Params{{Key: "chainId", Value: tt.pathChainID}}
			if tt.clientID != "" {
				requestContext.Request.Header.Set("client-id", tt.clientID)
			}

			handler(context.Background(), requestContext)
			require.NoError(t, collector.Close(context.Background()))

			records := producer.snapshot()
			require.Equal(t, tt.pathChainID, rpcHandler.chainID)
			require.Len(t, records, 1)
			require.Equal(t, tt.expectedID, records[0].ClientID)
			require.Equal(t, tt.expectedBase, records[0].ChainID)
			require.GreaterOrEqual(t, records[0].TimeMS, int64(2))
		})
	}
}

func TestRPCRequestHandlerDisabled(t *testing.T) {
	rpcHandler := &fakeRPCRequestHandler{}
	handler := newRPCRequestHandler(rpcHandler, nil)
	requestContext := app.NewContext(1)
	requestContext.Params = param.Params{{Key: "chainId", Value: "1"}}

	handler(context.Background(), requestContext)
	require.Equal(t, "1", rpcHandler.chainID)
}

func TestGracefulExitWaitCoversAllUpstreamAttempts(t *testing.T) {
	cfg := &types.Config{
		DefaultRPCTimeout:      5000,
		RPCMethodTimeoutConfig: map[string]int{"eth_call": 12000},
		ConnDialTimeout:        3000,
		ConnMaxWaitTimeout:     1000,
	}
	require.Equal(t, 53*time.Second, gracefulExitWait(cfg))
}

func TestGracefulExitWaitSaturatesOnOverflow(t *testing.T) {
	cfg := &types.Config{DefaultRPCTimeout: int(^uint(0) >> 1)}
	require.Equal(t, maxExitWait, gracefulExitWait(cfg))
}
