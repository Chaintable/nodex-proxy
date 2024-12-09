package roundrobin

import (
	"context"
	"testing"

	"github.com/Chaintable/nodex-proxy/lb/lbnode"
	"github.com/Chaintable/nodex-proxy/utils"
	"github.com/stretchr/testify/assert"
)

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
			nodes: []*lbnode.Node{
				lbnode.New("test_1", "192.168.1.2", 8080, 1),
				lbnode.New("test_2", "192.168.1.3", 8080, 1),
				lbnode.New("test_3", "192.168.1.4", 8080, 1),
			},
			args: args{
				times: 10,
			},
			want: []string{
				"test_1", "test_2", "test_3", "test_1", "test_2",
				"test_3", "test_1", "test_2", "test_3", "test_1"},
			wantErr: false,
		},
		{
			name: "success_diff_weight",
			nodes: []*lbnode.Node{
				lbnode.New("test_1", "192.168.1.2", 8080, 1),
				lbnode.New("test_2", "192.168.1.3", 8080, 2),
				lbnode.New("test_3", "192.168.1.4", 8080, 3),
			},
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
			r := New()
			testNodes(t, tt.nodes, []string{"test_1", "test_2", "test_3"})

			var gots []string
			for i := 0; i < tt.args.times; i++ {
				got, err := r.GetNode(context.Background(), tt.nodes, tt.args.requestKey)
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
