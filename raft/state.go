package raft

import (
	"sync"
	"sync/atomic"
)

type State uint32

const (
	Follower State = iota
	Candidate
	Leader
	Shutdown
)

type srvState struct {
	currentTerm uint64

	state State

	wg sync.WaitGroup
}

func (s *srvState) getState() State {
	stateAddr := (*uint32)(&s.state)
	return State(atomic.LoadUint32(stateAddr))
}

func (s *srvState) setState(state State) {
	stateAddr := (*uint32)(&s.state)
	atomic.StoreUint32(stateAddr, uint32(state))
}

func (s *srvState) getCurrentTerm() uint64 {
	return atomic.LoadUint64(&s.currentTerm)
}

func (s *srvState) setCurrentTerm(term uint64) {
	atomic.StoreUint64(&s.currentTerm, term)
}

func (s *srvState) goFunc(f func()) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		f()
	}()
}
