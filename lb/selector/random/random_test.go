package random

import (
	"reflect"
	"testing"

	"github.com/Chaintable/nodex-proxy/lb/lbnode"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

func TempPickNodes(blockContext *types.BlockContext, blockHeight *hexutil.Big, archiveNodes []*lbnode.Node, stateNodes []*lbnode.Node) []*lbnode.Node {
	return append(stateNodes, archiveNodes...)
}
func TestRandom_GetNode(t *testing.T) {
	type args struct {
		requestKey string
	}
	tempNodes1 := []*lbnode.Node{
		lbnode.New("test_1", "192.168.8.2", 80, 1),
		lbnode.New("test_2", "192.168.8.3", 80, 1),
	}

	tempNodes2 := []*lbnode.Node{
		lbnode.New("test_1", "192.168.8.2", 80, 2),
		lbnode.New("test_2", "192.168.8.3", 80, 1),
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
			r := New(TempPickNodes)
			r.UpsertNode(nil, "0x1", 2, tt.fields[0])
			r.UpsertNode(nil, "0x1", 2, tt.fields[1])
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
