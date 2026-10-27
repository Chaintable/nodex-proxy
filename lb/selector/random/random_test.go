package random

import (
	"context"
	"reflect"
	"testing"

	"github.com/Chaintable/nodex-proxy/discovery"
	"github.com/Chaintable/nodex-proxy/lb/jsonrpc"
	"github.com/Chaintable/nodex-proxy/lb/lbnode"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/stretchr/testify/require"
)

func TempPickNodes(getBlockContext func() *types.BlockContext, blockHeight *hexutil.Big, archiveNodes []*lbnode.Node, stateNodes []*lbnode.Node, nativeNodes []*lbnode.Node, forceArchive bool, forceNative bool) []*lbnode.Node {
	return append(stateNodes, archiveNodes...)
}

func TestRandom_GetNode(t *testing.T) {
	type args struct {
		requestKey string
	}
	tempNode1_1, _ := lbnode.New("test_1", "192.168.8.2", 80, 1, discovery.NodeTypeArchive)
	tempNode1_2, _ := lbnode.New("test_2", "192.168.8.3", 80, 1, discovery.NodeTypeArchive)
	tempNodes1 := []*lbnode.Node{
		tempNode1_1,
		tempNode1_2,
	}

	tempNode2_1, _ := lbnode.New("test_1", "192.168.8.2", 80, 2, discovery.NodeTypeArchive)
	tempNode2_2, _ := lbnode.New("test_2", "192.168.8.3", 80, 1, discovery.NodeTypeArchive)
	tempNodes2 := []*lbnode.Node{
		tempNode2_1,
		tempNode2_2,
	}
	tests := []struct {
		name    string
		fields  []*lbnode.Node
		args    args
		want    *lbnode.Node
		wantErr bool
	}{
		{name: "Test1", fields: tempNodes1, args: args{requestKey: " Test1"}, want: tempNodes1[0], wantErr: false},
		{name: "Test2", fields: tempNodes2, args: args{requestKey: " Test2"}, want: tempNodes2[0], wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := New(TempPickNodes, nil)
			r.UpsertNode(context.TODO(), "0x1", 2, tt.fields[0])
			r.UpsertNode(context.TODO(), "0x1", 2, tt.fields[1])
			got, err := r.GetNode(&types.RequestContext{ChainId: "0x1"}, tt.args.requestKey)
			if (err != nil) != tt.wantErr {
				t.Errorf("Random.getNode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) && !reflect.DeepEqual(got, tt.fields[1]) {
				t.Errorf("Random.getNode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRandomGetNodeHonorsWeights(t *testing.T) {
	gs := jsonrpc.NewGatewayStrategy()
	gs.UpdateGateway("1", discovery.Gateway{Weights: []discovery.WeightInfo{
		{NodeKey: "zero", Weight: 0},
		{NodeKey: "one", Weight: 1},
		{NodeKey: "three", Weight: 3},
	}})
	r := New(TempPickNodes, gs)
	for _, key := range []string{"zero", "one", "three"} {
		node, err := lbnode.New(key, "10.0.0.1", 80, 100, discovery.NodeTypeState)
		require.NoError(t, err)
		node.SetState(1)
		require.NoError(t, r.UpsertNode(context.Background(), "1", discovery.NodeTypeState, node))
	}

	const rounds = 40000
	hits := map[string]int{}
	for range rounds {
		node, err := r.GetNode(&types.RequestContext{ChainId: "1", IsBatch: true}, "")
		require.NoError(t, err)
		hits[node.Key()]++
	}
	require.Zero(t, hits["zero"], "a zero-weight node must never be selected")
	require.InDelta(t, 0.25, float64(hits["one"])/rounds, 0.02)
	require.InDelta(t, 0.75, float64(hits["three"])/rounds, 0.02)
}
