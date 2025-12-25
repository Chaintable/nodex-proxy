package roundrobin

import (
	"testing"

	"github.com/Chaintable/nodex-proxy/discovery"
	"github.com/Chaintable/nodex-proxy/lb/lbnode"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/Chaintable/nodex-proxy/utils"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/stretchr/testify/assert"
)

func TempPickNodes(blockContext *types.BlockContext, blockHeight *hexutil.Big, archiveNodes []*lbnode.Node, stateNodes []*lbnode.Node, nativeNodes []*lbnode.Node, forceArchive bool, forceNative bool) []*lbnode.Node {
	return append(stateNodes, archiveNodes...)
}

func TestRoundRobin_GetNode(t *testing.T) {
	type args struct {
		requestKey string
		times      int
	}
	tests := []struct {
		name    string
		nodes   []*lbnode.Node
		args    args
		want    []string
		wantErr bool
	}{
		{
			name: "success_same_weight",
			nodes: func() []*lbnode.Node {
				node1, _ := lbnode.New("test_1", "192.168.1.2", 8080, 1, discovery.NodeTypeArchive)
				node2, _ := lbnode.New("test_2", "192.168.1.3", 8080, 1, discovery.NodeTypeArchive)
				node3, _ := lbnode.New("test_3", "192.168.1.4", 8080, 1, discovery.NodeTypeArchive)
				return []*lbnode.Node{node1, node2, node3}
			}(),
			args: args{
				times: 10,
			},
			want: []string{
				"test_1", "test_2", "test_3", "test_1", "test_2",
				"test_3", "test_1", "test_2", "test_3", "test_1",
			},
			wantErr: false,
		},
		{
			name: "success_diff_weight",
			nodes: func() []*lbnode.Node {
				node1, _ := lbnode.New("test_1", "192.168.1.2", 8080, 1, discovery.NodeTypeArchive)
				node2, _ := lbnode.New("test_2", "192.168.1.3", 8080, 2, discovery.NodeTypeArchive)
				node3, _ := lbnode.New("test_3", "192.168.1.4", 8080, 3, discovery.NodeTypeArchive)
				return []*lbnode.Node{node1, node2, node3}
			}(),
			args: args{
				times: 12,
			},
			want: []string{
				"test_3", "test_2", "test_3", "test_1", "test_2", "test_3",
				"test_3", "test_2", "test_3", "test_1", "test_2", "test_3",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := New(TempPickNodes)
			for _, node := range tt.nodes {
				r.UpsertNode(nil, "0x1", 2, node)
			}
			testNodes(t, tt.nodes, []string{"test_1", "test_2", "test_3"})

			var gots []string
			for i := 0; i < tt.args.times; i++ {
				got, err := r.GetNode(&types.RequestContext{ChainId: "0x1"}, tt.args.requestKey)
				if tt.wantErr {
					assert.Error(t, err)
					assert.Equal(t, utils.ErrNoAvailableNode, err)
				} else {
					assert.NoError(t, err)
					gots = append(gots, got.Key())
				}
			}
			assert.Equal(t, tt.want, gots)
		})
	}
}

func testNodes(t *testing.T, nodes []*lbnode.Node, expect []string) {
	var keys []string
	for _, v := range nodes {
		keys = append(keys, v.Key())
	}
	assert.Equal(t, expect, keys)
}
