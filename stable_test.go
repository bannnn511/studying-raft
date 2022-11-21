package studying_raft

import (
	"reflect"
	"sync"
	"testing"
)

func TestMapStorage_Get(t *testing.T) {

	type fields struct {
		mu      sync.Mutex
		mByte   map[string][]byte
		mUint64 map[string]uint64
	}
	type args struct {
		key []byte
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []byte
	}{
		{
			name: "test",
			fields: fields{
				mByte:   nil,
				mUint64: nil,
			},
			args: args{key: keyLastVoteCand},
			want: []byte("hello"),
		},
	}

	ms := NewMapStorage()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			ms.Set(keyLastVoteCand, []byte("hello"))
			got, err := ms.Get(tt.args.key)
			if err != nil {
				t.Errorf("Get() error = %v", err)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() got = %s, want %s", got, tt.want)
			}
		})
	}
}
