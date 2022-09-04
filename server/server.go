package server

import (
	"github.com/zhihanii/go-drl/raft"
	"github.com/zhihanii/go-drl/ratelimit"
	"github.com/zhihanii/go-drl/server/protocol"
)

type Server struct {
	raft *raft.Raft

	bucket *ratelimit.Bucket

	transport Transport

	reqCh chan *protocol.Request

	shutdownCh chan struct{}
}

func New(config *Config) *Server {
	transport, err := NewNetworkTransport(config.Addr)
	if err != nil {
		panic(err)
	}

	r := raft.New(config.RaftConfig, raft.NewInmemStore())

	s := &Server{
		raft:       r,
		bucket:     ratelimit.NewBucketWithRateAndClock(config.BucketConfiguration.Rate, config.BucketConfiguration.Capacity, nil),
		transport:  transport,
		reqCh:      transport.Consumer(),
		shutdownCh: make(chan struct{}),
	}
	go s.run()

	return s
}

func (s *Server) run() {
	for {
		select {
		case req := <-s.reqCh:
			s.processReq(req)

		case <-s.shutdownCh:
			return
		}
	}
}

func (s *Server) processReq(req *protocol.Request) {
	switch cmd := req.Command.(type) {
	case *protocol.TakeRequest:
		s.handleTake(req, cmd)

	default:
		req.Respond(nil, protocol.ErrCommand)
	}
}
