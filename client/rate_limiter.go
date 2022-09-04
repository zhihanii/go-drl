package client

import (
	"go-drl/server/protocol"
	"time"
)

type leaderState struct {
	LeaderId   string
	LeaderAddr string
}

type RateLimiter struct {
	config *Config

	bucket *Bucket

	state *leaderState

	transport Transport

	shutdownCh chan struct{}
}

func NewRateLimiter(config *Config) *RateLimiter {
	l := &RateLimiter{
		config: config,
		bucket: &Bucket{},
		state: &leaderState{
			LeaderId:   config.LeaderId,
			LeaderAddr: config.LeaderAddr,
		},
		shutdownCh: make(chan struct{}),
	}
	l.transport = NewNetworkTransport(l)

	go l.run()
	return l
}

//func (l *RateLimiter) Take(n int64) (time.Duration, bool) {
//
//}
//
//func (l *RateLimiter) TakeMaxWait(n int64, maxWait time.Duration) (time.Duration, bool) {
//
//}

func (l *RateLimiter) setLeaderState(state *leaderState) {
	l.state = state
}

func (l *RateLimiter) Take(n int64) bool {
	return l.bucket.Take(n)
}

func (l *RateLimiter) run() {
	ticker := time.Tick(l.config.TakeInterval)

	for {
		select {
		case <-ticker:
			l.takeRequest()

		case <-l.shutdownCh:
			return
		}
	}
}

func (l *RateLimiter) takeRequest() {
	var (
		req  protocol.TakeRequest
		resp protocol.TakeResponse
		err  error
	)
	req = protocol.TakeRequest{
		RequestHeader: protocol.RequestHeader{
			ApiKey: int16(protocol.Take),
		},
	}

	err = l.transport.Take(&req, &resp)
	if err != nil {
		//
		return
	}

	l.bucket.UpdateTokens(resp.Tokens)
}
