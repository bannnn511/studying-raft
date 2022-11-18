package studying_raft

import (
	"testing"

	"go.uber.org/zap"
)

func TestRaft_slog(t *testing.T) {

	type args struct {
		format string
		args   []interface{}
	}
	tests := []struct {
		name string
		args args
	}{
		{
			"fetasetas",
			args{
				format: "entering follower state",
				args:   nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Raft{}
			logger, _ := zap.NewProduction()
			r.logger = logger.Sugar()
			r.slog(tt.args.format, "leader", 1, "follower", 2)
		})
	}
}

func TestRaft_random(t *testing.T) {

}
