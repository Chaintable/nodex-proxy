package jsonrpc

import (
	"errors"
	"net/http"
	"testing"

	"github.com/Chaintable/nodex-proxy/types"
	"github.com/stretchr/testify/assert"
)

func TestMetricChainLabels(t *testing.T) {
	cases := []struct {
		name          string
		ctx           *types.RequestContext
		expectChainID string
		expectVersion string
	}{
		{
			name: "base chain without version",
			ctx: &types.RequestContext{
				BaseChainId: "1",
				ChainId:     "1",
			},
			expectChainID: "1",
			expectVersion: "",
		},
		{
			name: "override version from chain id",
			ctx: &types.RequestContext{
				BaseChainId: "1",
				ChainId:     "1-v1",
			},
			expectChainID: "1",
			expectVersion: "v1",
		},
		{
			name: "explicit chain uuid",
			ctx: &types.RequestContext{
				BaseChainId: "1",
				ChainUUID:   "beta",
				ChainId:     "1-beta",
			},
			expectChainID: "1",
			expectVersion: "beta",
		},
		{
			name: "fallback to chain id when base empty",
			ctx: &types.RequestContext{
				ChainId: "56",
			},
			expectChainID: "56",
			expectVersion: "",
		},
		{
			name: "non-prefixed version not extracted",
			ctx: &types.RequestContext{
				BaseChainId: "1",
				ChainId:     "1v1",
			},
			expectChainID: "1",
			expectVersion: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			chainID, chainVersion := metricChainLabels(tc.ctx)
			assert.Equal(t, tc.expectChainID, chainID)
			assert.Equal(t, tc.expectVersion, chainVersion)
		})
	}
}

func TestFailureNodeAddr(t *testing.T) {
	cases := []struct {
		name string
		ctx  *types.RequestContext
		want string
	}{
		{name: "nil context", ctx: nil, want: "unknown"},
		{name: "empty node addr", ctx: &types.RequestContext{}, want: "unknown"},
		{name: "blank node addr", ctx: &types.RequestContext{TargetNodeAddr: " "}, want: "unknown"},
		{name: "node addr", ctx: &types.RequestContext{TargetNodeAddr: "10.0.0.12:8545"}, want: "10.0.0.12:8545"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, failureNodeAddr(tc.ctx))
		})
	}
}

func TestShouldRecordResponsePayloadSize(t *testing.T) {
	cases := []struct {
		name             string
		statusCode       int
		ctx              *types.RequestContext
		responseBodySize int
		want             bool
	}{
		{
			name:             "success response with body",
			statusCode:       http.StatusOK,
			ctx:              &types.RequestContext{},
			responseBodySize: 42,
			want:             true,
		},
		{
			name:             "empty body",
			statusCode:       http.StatusOK,
			ctx:              &types.RequestContext{},
			responseBodySize: 0,
			want:             false,
		},
		{
			name:             "request context error",
			statusCode:       http.StatusOK,
			ctx:              &types.RequestContext{Error: errors.New("upstream error")},
			responseBodySize: 42,
			want:             false,
		},
		{
			name:             "non 2xx response",
			statusCode:       http.StatusBadGateway,
			ctx:              &types.RequestContext{},
			responseBodySize: 42,
			want:             false,
		},
		{
			name:             "nil request context",
			statusCode:       http.StatusOK,
			ctx:              nil,
			responseBodySize: 42,
			want:             false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, shouldRecordResponsePayloadSize(tc.statusCode, tc.ctx, tc.responseBodySize))
		})
	}
}
