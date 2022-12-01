package studying_raft

import (
	"reflect"
	"testing"
)

func TestLog_deleteRange(t *testing.T) {
	type args struct {
		min int
		max int
	}
	var tests = []struct {
		name string
		l    Log
		args args
		want *Log
	}{
		{
			name: "test delete",
			l: []CommitEntry{
				{
					Index: 0,
					Term:  0,
				},
				{
					Index: 1,
					Term:  1,
				},
				{
					Index: 2,
					Term:  2,
				},
				{
					Index: 3,
					Term:  3,
				},
				{
					Index: 4,
					Term:  4,
				},
				{
					Index: 5,
					Term:  5,
				},
			},
			args: args{
				min: 2,
				max: 4,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.l.deleteRange(tt.args.min, tt.args.max); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("deleteRange() = %v, want %v", got, tt.want)
			}
		})
	}
}
