package utils

import "testing"

func Test_range_random(t *testing.T) {
	type args struct {
		min int64
		max int64
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{"Test1", args{1, 10}, 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RangeRandom(tt.args.min, tt.args.max); got > tt.want {
				t.Errorf("RangeRandom() = %v, want %v", got, tt.want)
			}
		})
	}
}
