package studying_raft

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Server struct {
	mu       sync.Mutex
	serverId int
	peerIds  []int

	raft     *Raft
	rpcProxy *RpcProxy

	rpcServer *rpc.Server
	listener  net.Listener

	commitCh    chan<- CommitEntry
	peerClients map[int]*rpc.Client

	readyCh <-chan struct{}
	quitCh  chan struct{}
	wg      sync.WaitGroup
}

func NewServer(serverId int, peerIds []int, readyCh <-chan struct{}, commitCh chan<- CommitEntry) *Server {
	s := new(Server)
	s.serverId = serverId
	s.peerIds = peerIds
	s.peerClients = make(map[int]*rpc.Client, len(peerIds))
	s.commitCh = commitCh
	s.readyCh = readyCh
	s.quitCh = make(chan struct{})

	return s
}

func (s *Server) Serve() {
	s.mu.Lock()

	s.rpcServer = rpc.NewServer()
	s.rpcProxy = &RpcProxy{raft: s.raft}
	err := s.rpcServer.RegisterName("Raft", s.rpcProxy)
	if err != nil {
		log.Fatal("RegisterName error", err)
	}

	s.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[%v] listening at %s", s.serverId, s.listener.Addr())
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		for {
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.quitCh:
					return
				default:
					log.Fatal("accept error", err)
				}
			}
			s.wg.Add(1)
			go func() {
				s.rpcServer.ServeConn(conn)
			}()
		}
	}()

}

func (s *Server) DisconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for id := range s.peerClients {
		if s.peerClients[id] != nil {
			s.peerClients[id].Close()
			s.peerClients[id] = nil
		}
	}
}

func (s *Server) Shutdown() {
	s.raft.shutDownCh <- struct{}{}
	close(s.quitCh)
	s.listener.Close()
	s.wg.Wait()
}

func (s *Server) GetListenAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listener.Addr()
}

func (s *Server) ConnectToPeer(peerId int, addr net.Addr) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] == nil {
		client, err := rpc.Dial("tcp", addr.String())
		if err != nil {
			return err
		}
		s.peerClients[peerId] = client
	}

	return nil
}

func (s *Server) DisconnectPeer(peerId int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] != nil {
		err := s.peerClients[peerId].Close()
		if err != nil {
			return err
		}

		s.peerClients[peerId] = nil
	}

	return nil
}

func (s *Server) Call(id int, serviceMethod string, args interface{}, reply interface{}) error {
	s.mu.Lock()
	peer := s.peerClients[id]
	s.mu.Unlock()

	if peer == nil {
		fmt.Errorf("call client %d after it closed", id)
	} else {
		peer.Call(serviceMethod, args, reply)
	}
	return nil
}

type RpcProxy struct {
	raft *Raft
}

func (rpp *RpcProxy) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)
		if dice == 9 {
			rpp.raft.dlog("drop RequestVote")
			return fmt.Errorf("RPC failed")
		} else if dice == 8 {
			rpp.raft.dlog("delay RequestVote")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpp.raft.RequestVote(args, reply)
}

func (rpp *RpcProxy) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)
		if dice == 9 {
			rpp.raft.dlog("drop AppendEntries")
			return fmt.Errorf("RPC failed")
		} else if dice == 8 {
			rpp.raft.dlog("delay AppendEntries")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpp.raft.AppendEntries(args, reply)
}
