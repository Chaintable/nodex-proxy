package lb

import (
	"context"
	"fmt"
	"testing"

	ejrpc "github.com/Chaintable/nodex-proxy/jsonrpc"
	"github.com/Chaintable/nodex-proxy/lib/log"
	"github.com/Chaintable/nodex-proxy/types"
	nJson "github.com/bytedance/sonic"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/stretchr/testify/require"
)

func init() {
	// Initialize logger for tests
	log.InitLogger("error")
}

var (
	txStr1 = `
						{
                                "to": "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                                "data": "0x70a082310000000000000000000000000cecb15396825A895FF8DA8fC2D2C77A234dcCee"
                        }`

	overrides = `
						{
								"0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee":{
									"nonce":"0x2345",
									"balance": "0xaeeeee00",
									"code": "0x70a082310000000000000000000000000cecb15396825A895FF8DA8fC2D2C77A234dcCee",
									"state":{
										"0x32b3d41d31b51513cc77eee858049a0d832cc9944a384dbe307d1992806492b4":"0x32b3d41d31b51513cc77eee858049a0d832cc9944a384dbe307d1992806492b4"
									},
									"stateDiff":{
										"0x32b3d41d31b51513cc77eee858049a0d832cc9944a384dbe307d1992806492b4":"0x32b3d41d31b51513cc77eee858049a0d832cc9944a384dbe307d1992806492b4"
									},
									"movePrecompileToAddress":"0x61070f0fee7b188eed23e32692f09ab64c3cceeb"
								}
                        }`

	blockContext1 = `
						{
							"block_id": "0x5b793d5d9f03377d0c4db3386aa3d17b2efff06f5825a531c082bcbccac5b3dc",
							"type": "Contains"
						}
						`

	blockContext2 = `
						{
							"block_id": "latest",
							"type": "Contains"
						}
						`

	multiCallParamsTemplate = `
		{
			"jsonrpc": "2.0",
			"method": "eth_multiCall",
			"id": 1,
			"params": [
                [
                    %s
                ],
                "latest",
                false,
                false,
                true,
				%s,
				%s
        	]
		}		
		
`

	multiCallParamsRight1 = fmt.Sprintf(multiCallParamsTemplate, txStr1, overrides, blockContext1)
	multiCallParamsRight2 = fmt.Sprintf(multiCallParamsTemplate, txStr1, overrides, blockContext2)
)

func TestParseBlockContext(t *testing.T) {
	// 1. get request and parse

	var params ejrpc.RequestObject
	err := nJson.Unmarshal([]byte(multiCallParamsRight1), &params)
	require.NoError(t, err)

	lb := &LoadBalancer{}
	ctx := lb.ParseBlockContext([]*ejrpc.RequestObject{&params})
	require.NotNil(t, ctx)

	byt, err := nJson.Marshal(ctx)
	require.NoError(t, err)
	t.Log(string(byt))
}

func TestBeforeProcessForceArchiveHeader(t *testing.T) {
	tests := []struct {
		name        string
		headerValue string
		setHeader   bool
		wantArchive bool
	}{
		{name: "missing header", wantArchive: false},
		{name: "true", headerValue: "true", setHeader: true, wantArchive: true},
		{name: "uppercase true", headerValue: "TRUE", setHeader: true, wantArchive: false},
		{name: "one", headerValue: "1", setHeader: true, wantArchive: false},
		{name: "yes", headerValue: "yes", setHeader: true, wantArchive: false},
		{name: "on", headerValue: "on", setHeader: true, wantArchive: false},
		{name: "false", headerValue: "false", setHeader: true, wantArchive: false},
		{name: "zero", headerValue: "0", setHeader: true, wantArchive: false},
		{name: "invalid", headerValue: "archive", setHeader: true, wantArchive: false},
	}

	lb := &LoadBalancer{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var request protocol.Request
			if tt.setHeader {
				request.Header.Set(types.NodexForceArchive, tt.headerValue)
			}

			requestContext := lb.beforeProcess(context.Background(), &request)
			require.Equal(t, tt.wantArchive, requestContext.Archive)
		})
	}
}

func BenchmarkMarshal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var params ejrpc.RequestObject
		err := nJson.Unmarshal([]byte(multiCallParamsRight1), &params)
		require.NoError(b, err)

		lb := &LoadBalancer{}
		ctx := lb.ParseBlockContext([]*ejrpc.RequestObject{&params})
		require.NotNil(b, ctx)
	}
}

func BenchmarkMarshal2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var params ejrpc.RequestObject
		err := nJson.Unmarshal([]byte(multiCallParamsRight1), &params)
		require.NoError(b, err)

		lb := &LoadBalancer{}
		ctx := lb.ParseBlockContext([]*ejrpc.RequestObject{&params})
		require.NotNil(b, ctx)
	}
}
