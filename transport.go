package studying_raft

type Transport interface {
	Call(id string, serviceMethod string, args interface{}, reply interface{}) error
}
