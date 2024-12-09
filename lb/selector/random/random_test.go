package random

import (
	"context"

	"reflect"
	"testing"

	"github.com/Chaintable/nodex-proxy/lb/lbnode"
)

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
			r := New()
			got, err := r.GetNode(context.Background(), tt.fields, tt.args.requestKey)
			if (err != nil) != tt.wantErr {
				t.Errorf("Random.GetNode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) && !reflect.DeepEqual(got, tt.fields[1]) {
				t.Errorf("Random.GetNode() = %v, want %v", got, tt.want)
			}
		})
	}
}
