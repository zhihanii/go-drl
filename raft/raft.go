package raft

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

var (
	keyCurrentTerm  = []byte("CurrentTerm")
	keyLastVoteTerm = []byte("LastVoteTerm")
	keyLastVoteCand = []byte("LastVoteCand")
)

type Raft struct {
	srvState

	ctx context.Context

	conf atomic.Value

	configurations configurations

	configurationChangeCh chan *configurationChangeFuture

	latestConfiguration atomic.Value

	transport Transport

	lastContactLock sync.RWMutex
	lastContact     time.Time

	leaderLock  sync.RWMutex
	leaderId    string
	leaderAddr  string
	leaderCh    chan bool
	leaderState leaderState

	candidateFromLeadershipTransfer bool

	localId   string
	localAddr string

	stable StableStore

	reqCh chan *Request

	shutdownLock sync.Mutex
	shutdown     bool
	shutdownCh   chan struct{}
}

type follower struct {
	curTerm         uint64
	peerLock        sync.RWMutex
	peer            ServerInfo
	stopCh          chan struct{}
	lastContactLock sync.RWMutex
	lastContact     time.Time
	notifyCh        chan struct{}
	stepDown        chan struct{}
}

func (f *follower) LastContact() time.Time {
	f.lastContactLock.RLock()
	last := f.lastContact
	f.lastContactLock.RUnlock()
	return last
}

func (f *follower) setLastContact() {
	f.lastContactLock.Lock()
	f.lastContact = time.Now()
	f.lastContactLock.Unlock()
}

type leaderState struct {
	leadershipTransferInProgress int32
	followerState                map[string]*follower
	stepDown                     chan struct{}
}

func (r *Raft) config() Config {
	return r.conf.Load().(Config)
}

func (r *Raft) getLocalId() string {
	return r.config().LocalId
}

func (r *Raft) getLocalAddr() string {
	return r.config().LocalAddr
}

func (r *Raft) minVotes() int {
	voters := 0
	for _, server := range r.configurations.latest.Servers {
		if server.Suffrage == Voter {
			voters++
		}
	}
	return voters/2 + 1
}

func (r *Raft) LeaderCh() chan bool {
	return r.leaderCh
}

func (r *Raft) setLeader(leaderAddr, leaderId string) {
	r.leaderLock.Lock()
	//oldLeaderAddr := r.leaderAddr
	r.leaderAddr = leaderAddr
	//oldLeaderId := r.leaderId
	r.leaderId = leaderId
	r.leaderLock.Unlock()
	//if oldLeaderAddr != leaderAddr || oldLeaderId != leaderId {
	//
	//}
}

func (r *Raft) GetLeader() (string, string) {
	return r.leaderId, r.leaderAddr
}

func (r *Raft) IsLeader() bool {
	r.leaderLock.RLock()
	defer r.leaderLock.RUnlock()
	return r.localId == r.leaderId && r.localAddr == r.leaderAddr
}

func (r *Raft) setLastContact() {
	r.lastContactLock.Lock()
	r.lastContact = time.Now()
	r.lastContactLock.Unlock()
}

func (r *Raft) LastContact() time.Time {
	r.lastContactLock.RLock()
	last := r.lastContact
	r.lastContactLock.RUnlock()
	return last
}

func (r *Raft) LeaderWithId() (string, string) {
	r.leaderLock.RLock()
	leaderAddr := r.leaderAddr
	leaderId := r.leaderId
	r.leaderLock.RUnlock()
	return leaderAddr, leaderId
}

func (r *Raft) startStopHeartbeat() {
	inConfig := make(map[string]bool, len(r.configurations.latest.Servers))

	for _, server := range r.configurations.latest.Servers {
		if server.Id == r.localId {
			continue
		}

		inConfig[server.Id] = true

		f, ok := r.leaderState.followerState[server.Id]
		if !ok {
			f = &follower{
				peer:     server,
				stopCh:   make(chan struct{}, 1),
				curTerm:  r.getCurrentTerm(),
				notifyCh: make(chan struct{}, 1),
				stepDown: r.leaderState.stepDown,
			}

			r.leaderState.followerState[server.Id] = f
			r.goFunc(func() {
				r.doHeartbeat(f)
			})
		} else {
			f.peerLock.RLock()
			peer := f.peer
			f.peerLock.RUnlock()

			if peer.Address != server.Address {
				//log
				f.peerLock.Lock()
				f.peer = server
				f.peerLock.Unlock()
			}
		}
	}

	for serverId, f := range r.leaderState.followerState {
		if inConfig[serverId] {
			continue
		}
		f.stopCh <- struct{}{}
		close(f.stopCh)
		delete(r.leaderState.followerState, serverId)
	}
}

func (r *Raft) doHeartbeat(f *follower) {
	stopHeartbeat := make(chan struct{})
	defer close(stopHeartbeat)
	r.goFunc(func() {
		r.sendHeartbeat(f, stopHeartbeat)
	})
	select {
	case <-f.stopCh:
		return
	case <-r.shutdownCh:
		return
	}
}

func (r *Raft) processReq(req *Request) {
	switch cmd := req.Command.(type) {
	case *RequestVoteRequest:
		r.requestVote(req, cmd)

	case *ConfigChangeRequest:
		r.configChange(req, cmd)

	case *ShutdownRequest:
		r.doShutdown(req, cmd)

	default:
		req.respond(nil, errors.New("unexpected command"))
	}
}

const failureWait = 10 * time.Millisecond

const maxFailureScale = 12

func (r *Raft) sendHeartbeat(f *follower, stopCh chan struct{}) {
	var failures uint64
	req := &HeartbeatRequest{
		RequestHeader: RequestHeader{
			ApiKey: int16(Heartbeat),
			Id:     r.getLocalId(),
			Addr:   r.getLocalAddr(),
		},
		Term: f.curTerm,
	}

	var resp HeartbeatResponse
	for {
		select {
		case <-f.notifyCh:
		case <-randomTimeout(r.config().HeartbeatTimeout / 10):
		case <-stopCh:
			return
		}

		f.peerLock.RLock()
		peer := f.peer
		f.peerLock.RUnlock()

		if err := r.transport.Heartbeat(peer.Id, peer.Address, req, &resp); err != nil {
			failures++

			if failures > 15 {
				future := r.requestConfigChange(configurationChangeRequest{
					command:  RemoveServer,
					serverId: peer.Id,
				}, 0)
				err := future.Error()
				if err != nil {
				}
				return
			}

			select {
			case <-time.After(backoff(failureWait, failures, maxFailureScale)):
			case <-stopCh:
			}
		} else {
			if failures > 0 {

			}
			f.setLastContact()
			failures = 0

		}
	}
}

func (r *Raft) handleHeartbeat(req *Request) {
	select {
	case <-r.shutdownCh:
		return
	default:
	}

	switch cmd := req.Command.(type) {
	case *HeartbeatRequest:
		r.heartbeat(req, cmd)
	default:
		req.respond(nil, errors.New("unexpected command"))
	}
}

func (r *Raft) heartbeat(req *Request, h *HeartbeatRequest) {
	var (
		resp *HeartbeatResponse
		err  error
	)
	resp = &HeartbeatResponse{
		RequestHeader: RequestHeader{
			Id:   r.localId,
			Addr: r.leaderAddr,
		},
		Term:    r.getCurrentTerm(),
		Success: false,
	}
	defer func() {
		req.respond(resp, err)
	}()

	if h.Term < r.getCurrentTerm() {
		return
	}

	if h.Term > r.getCurrentTerm() {
		r.setState(Follower)
		r.setCurrentTerm(h.Term)
		resp.Term = h.Term
	}

	if len(h.Addr) > 0 {
		r.setLeader(h.Addr, h.Id)
	}

	resp.Success = true
	r.setLastContact()
	return
}

func (r *Raft) requestVote(req *Request, c *RequestVoteRequest) {
	var (
		resp *RequestVoteResponse
		err  error
	)

	resp = &RequestVoteResponse{
		RequestHeader: RequestHeader{
			Id:   r.localId,
			Addr: r.localAddr,
		},
		Term:    r.getCurrentTerm(),
		Granted: false,
	}
	defer func() {
		req.respond(resp, err)
	}()

	var (
		candidate      = c.Addr
		candidateBytes = []byte(c.Addr)
	)

	if len(c.Id) > 0 {
		candidateId := c.Id
		if len(r.configurations.latest.Servers) > 0 && !hasVote(r.configurations.latest, candidateId) {
			//log.Println(319)
			return
		}
	}

	if leaderAddr, _ := r.LeaderWithId(); leaderAddr != "" && leaderAddr != candidate && !c.LeadershipTransfer {
		return
	}

	if c.Term < r.getCurrentTerm() {
		//log.Println(330)
		return
	}

	if c.Term > r.getCurrentTerm() {
		r.setState(Follower)
		r.setCurrentTerm(c.Term)
		resp.Term = c.Term
	}

	lastVoteTerm, err := r.stable.GetUint64(keyLastVoteTerm)
	if err != nil && err.Error() != "not found" {
		//log.Println(342)
		return
	}
	lastVoteCandBytes, err := r.stable.Get(keyLastVoteCand)
	if err != nil && err.Error() != "not found" {
		//log.Println(347)
		return
	}

	if lastVoteTerm == c.Term && lastVoteCandBytes != nil {
		if bytes.Compare(lastVoteCandBytes, candidateBytes) == 0 {
			resp.Granted = true
		}
		return
	}

	if err = r.persistVote(c.Term, candidateBytes); err != nil {
		//log.Println(360)
		return
	}

	log.Printf("server - id:%s grant vote for server:%s", r.localId, c.Id)
	resp.Granted = true
	r.setLastContact()
	return
}

func (r *Raft) configChange(req *Request, c *ConfigChangeRequest) {
	var (
		resp *ConfigChangeResponse
		err  error
	)

	resp = &ConfigChangeResponse{
		RequestHeader: RequestHeader{
			Id:   r.localId,
			Addr: r.localAddr,
		},
		Success: true,
	}
	defer func() {
		req.respond(resp, err)
	}()

	r.setLatestConfiguration(c.Configuration)
}

func (r *Raft) doShutdown(req *Request, c *ShutdownRequest) {
	var (
		resp *ShutdownResponse
		err  error
	)

	resp = &ShutdownResponse{
		RequestHeader: RequestHeader{
			Id:   r.localId,
			Addr: r.localAddr,
		},
		Success: true,
	}
	defer func() {
		req.respond(resp, err)
	}()

	r.setLeader("", "")
	r.configurations.committed = Configuration{}
	r.setLatestConfiguration(Configuration{})
	r.Shutdown()
}

func (r *Raft) persistVote(term uint64, candidate []byte) error {
	if err := r.stable.SetUint64(keyLastVoteTerm, term); err != nil {
		return err
	}
	if err := r.stable.Set(keyLastVoteCand, candidate); err != nil {
		return err
	}
	return nil
}

func (r *Raft) run() {
	for {
		select {
		case <-r.shutdownCh:
			r.setLeader("", "")
			return
		default:
		}

		switch r.getState() {
		case Follower:
			r.runFollower()
		case Candidate:
			r.runCandidate()
		case Leader:
			r.runLeader()
		}
	}
}

func (r *Raft) runFollower() {
	log.Printf("server - id:%r is running follower", r.localId)

	//leaderAddr, leaderId := r.LeaderWithId()
	heartbeatTimer := randomTimeout(r.config().HeartbeatTimeout)

	for r.getState() == Follower {
		select {
		case req := <-r.reqCh:
			r.processReq(req)

		case c := <-r.configurationChangeCh:
			c.respond(ErrNotLeader)

		case <-heartbeatTimer:
			hbTimeout := r.config().HeartbeatTimeout
			heartbeatTimer = randomTimeout(hbTimeout)

			lastContact := r.LastContact()
			if time.Now().Sub(lastContact) < hbTimeout {
				continue
			}

			log.Printf("server - id:%r heartbeat timeout", r.localId)

			//lastLeaderAddr, lastLeaderId := r.LeaderWithId()
			r.setLeader("", "")

			if hasVote(r.configurations.latest, r.localId) {
				r.setState(Candidate)
				return
			}

		case <-r.shutdownCh:
			return
		}
	}
}

func (r *Raft) runCandidate() {
	log.Printf("server - id:%r is running candidate", r.localId)
	voteCh := r.electSelf()

	defer func() {
		r.candidateFromLeadershipTransfer = false
	}()

	electionTimeout := r.config().ElectionTimeout
	electionTimer := randomTimeout(electionTimeout)

	grantedVotes := 0
	minVotes := r.minVotes()

	for r.getState() == Candidate {
		select {
		case req := <-r.reqCh:
			r.processReq(req)

		case vote := <-voteCh:
			if vote.Term > r.getCurrentTerm() {
				r.setState(Follower)
				r.setCurrentTerm(vote.Term)
				return
			}

			if vote.Granted {
				grantedVotes++
			}

			if grantedVotes >= minVotes {
				r.setState(Leader)
				r.setLeader(r.localAddr, r.localId)
				log.Printf("server - id:%r is leader", r.localId)
				return
			}

		case c := <-r.configurationChangeCh:
			c.respond(ErrNotLeader)

		case <-electionTimer:
			log.Printf("server - id:%r election timeout", r.localId)
			return

		case <-r.shutdownCh:
			return
		}
	}
}

func overrideNotifyBool(ch chan bool, v bool) {
	select {
	case ch <- v:
	case <-ch:
		select {
		case ch <- v:
		default:
			panic("race: channel was sent concurrently")
		}
	}
}

func (r *Raft) getLeadershipTransferInProgress() bool {
	v := atomic.LoadInt32(&r.leaderState.leadershipTransferInProgress)
	return v == 1
}

func (r *Raft) setupLeaderState() {
	r.leaderState.followerState = make(map[string]*follower)
	r.leaderState.stepDown = make(chan struct{}, 1)
}

func (r *Raft) runLeader() {
	log.Printf("server - id:%r is running leader", r.leaderId)
	//notify leader

	overrideNotifyBool(r.leaderCh, true)

	notify := r.config().NotifyCh

	if notify != nil {
		select {
		case notify <- true:
		case <-r.shutdownCh:
		}
	}

	r.setupLeaderState()

	stopCh := make(chan struct{})

	defer func() {
		close(stopCh)

		r.setLastContact()

		r.leaderLock.Lock()
		if r.leaderAddr == r.localAddr && r.leaderId == r.localId {
			r.leaderAddr = ""
			r.leaderId = ""
		}
		r.leaderLock.Unlock()

		overrideNotifyBool(r.leaderCh, false)

		if notify != nil {
			select {
			case notify <- false:
			case <-r.shutdownCh:
				select {
				case notify <- false:
				default:
				}
			}
		}
	}()

	r.startStopHeartbeat()

	//stepDown := false

	lease := time.After(r.config().LeaderLeaseTimeout)

	for r.getState() == Leader {
		select {
		case req := <-r.reqCh:
			r.processReq(req)

		case <-r.leaderState.stepDown:
			r.setState(Follower)

		case future := <-r.configurationChangeCh:
			r.handleConfigurationChange(future)

		case <-lease:

		case <-r.shutdownCh:
			return
		}
	}
}

type voteResult struct {
	RequestVoteResponse
	voterId string
}

func (r *Raft) electSelf() <-chan *voteResult {
	respCh := make(chan *voteResult, len(r.configurations.latest.Servers))

	r.setCurrentTerm(r.getCurrentTerm() + 1)
	req := &RequestVoteRequest{
		RequestHeader: RequestHeader{
			ApiKey: int16(RequestVote),
			Id:     r.localId,
			Addr:   r.localAddr,
		},
		Term:               r.getCurrentTerm(),
		LeadershipTransfer: r.candidateFromLeadershipTransfer,
	}

	askPeer := func(peer ServerInfo) {
		r.goFunc(func() {
			resp := &voteResult{
				voterId: peer.Id,
			}
			err := r.transport.RequestVote(peer.Id, peer.Address, req, &resp.RequestVoteResponse)
			if err != nil {
				resp.Term = req.Term
				resp.Granted = false
			}
			respCh <- resp
		})
	}

	for _, server := range r.configurations.latest.Servers {
		if server.Suffrage == Voter {
			if server.Id == r.localId {
				if err := r.persistVote(req.Term, []byte(req.Addr)); err != nil {
					//log
					return nil
				}
				respCh <- &voteResult{
					RequestVoteResponse: RequestVoteResponse{
						RequestHeader: RequestHeader{
							Id:   r.localId,
							Addr: r.localAddr,
						},
						Term:    req.Term,
						Granted: true,
					},
					voterId: r.localId,
				}
			} else {
				askPeer(server)
			}
		}
	}

	return respCh
}

const (
	AddVoter int8 = iota
	RemoveServer
)

type configurationChangeRequest struct {
	command       int8
	serverId      string
	serverAddress string
}

type Future interface {
	Error() error
}

type errorFuture struct {
	err error
}

func (e errorFuture) Error() error {
	return e.err
}

type deferError struct {
	err       error
	errCh     chan error
	responded bool
}

func (d *deferError) init() {
	d.errCh = make(chan error, 1)
}

func (d *deferError) Error() error {
	if d.responded {
		return d.err
	}
	select {
	case d.err = <-d.errCh:
	}
	return d.err
}

func (d *deferError) respond(err error) {
	if d.responded {
		return
	}
	d.errCh <- err
	close(d.errCh)
	d.responded = true
}

type configurationChangeFuture struct {
	deferError
	req configurationChangeRequest
}

func (r *Raft) requestConfigChange(req configurationChangeRequest, timeout time.Duration) Future {
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}
	future := &configurationChangeFuture{
		req: req,
	}
	future.init()
	select {
	case <-timer:
		return errorFuture{ErrEnqueueTimeout}
	case r.configurationChangeCh <- future:
		return future
	case <-r.shutdownCh:
		return errorFuture{ErrServerShutdown}
	}
}

func (r *Raft) handleConfigurationChange(future *configurationChangeFuture) {
	configuration, err := nextConfiguration(r.configurations.latest, future.req)
	if err != nil {
		future.respond(err)
		return
	}

	r.setLatestConfiguration(configuration)
	r.startStopHeartbeat()

	future.respond(nil)

	for _, server := range r.configurations.latest.Servers {
		if server.Id == r.localId {
			continue
		}

		if f, ok := r.leaderState.followerState[server.Id]; ok {
			r.sendConfigChange(f, configuration)
		}
	}
}

func (r *Raft) sendConfigChange(f *follower, configuration Configuration) {
	req := &ConfigChangeRequest{
		RequestHeader: RequestHeader{
			ApiKey: int16(ConfigChange),
			Id:     r.getLocalId(),
			Addr:   r.getLocalAddr(),
		},
		Configuration: configuration,
	}

	var resp ConfigChangeResponse

	f.peerLock.RLock()
	peer := f.peer
	f.peerLock.RUnlock()

	if err := r.transport.ConfigChange(peer.Id, peer.Address, req, &resp); err != nil {
	} else {
		f.setLastContact()
	}
}

func (r *Raft) handleShutdown(id string) error {
	var (
		address string
		found   = false
	)
	for _, server := range r.configurations.latest.Servers {
		if server.Id == id {
			address = server.Address
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("not found server id:%r", id)
	}

	r.sendShutdown(id, address)
	return nil
}

func (r *Raft) sendShutdown(id string, address string) {
	req := &ShutdownRequest{
		RequestHeader: RequestHeader{
			ApiKey: int16(RequestShutdown),
			Id:     r.getLocalId(),
			Addr:   r.getLocalAddr(),
		},
	}

	var resp ShutdownResponse

	if err := r.transport.ShutdownRequest(id, address, req, &resp); err != nil {

	}
}

func (r *Raft) setLatestConfiguration(c Configuration) {
	r.configurations.latest = c
	r.latestConfiguration.Store(c.Clone())
}

func (r *Raft) setCommittedConfiguration(c Configuration) {
	r.configurations.committed = c
}

func (r *Raft) getLatestConfiguration() Configuration {
	switch c := r.latestConfiguration.Load().(type) {
	case Configuration:
		return c
	default:
		return Configuration{}
	}
}
