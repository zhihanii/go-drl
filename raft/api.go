package raft

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrNotLeader      = errors.New("node is not leader")
	ErrServerShutdown = errors.New("server is already shutdown")
	ErrEnqueueTimeout = errors.New("timeout enqueue operation")
)

func New(conf *Config, stable StableStore) *Raft {
	currentTerm, err := stable.GetUint64(keyCurrentTerm)
	if err != nil {
		panic(fmt.Errorf("failed to load current term: %v", err))
	}

	transport, err := NewNetworkTransport(conf.LocalAddr)
	if err != nil {
		panic(err)
	}

	r := &Raft{
		localId:               conf.LocalId,
		localAddr:             conf.LocalAddr,
		configurationChangeCh: make(chan *configurationChangeFuture, 1),
		configurations:        configurations{},
		leaderCh:              make(chan bool, 1),
		reqCh:                 transport.Consumer(),
		shutdownCh:            make(chan struct{}),
		stable:                stable,
		transport:             transport,
	}

	r.conf.Store(*conf)

	r.setState(Follower)

	r.setCurrentTerm(currentTerm)

	transport.SetHeartbeatHandler(r.handleHeartbeat)

	err = r.Bootstrap(conf.Configuration)
	panic(err)

	return r
}

func (r *Raft) Bootstrap(configuration Configuration) error {
	if len(configuration.Servers) < 1 {
		return errors.New("at least 1 server")
	}
	r.configurations.committed = configuration
	r.setLatestConfiguration(configuration)
	r.goFunc(r.run)
	return nil
}

func (r *Raft) AddVoter(id string, address string, timeout time.Duration) Future {
	return r.requestConfigChange(configurationChangeRequest{
		command:       AddVoter,
		serverId:      id,
		serverAddress: address,
	}, timeout)
}

func (r *Raft) RemoveServer(id string, timeout time.Duration) Future {
	err := r.handleShutdown(id)
	if err != nil {

	}
	return r.requestConfigChange(configurationChangeRequest{
		command:  RemoveServer,
		serverId: id,
	}, timeout)
}

func (r *Raft) Shutdown() {
	r.shutdownLock.Lock()
	defer r.shutdownLock.Unlock()

	if !r.shutdown {
		close(r.shutdownCh)
		r.transport.Shutdown()
		r.shutdown = true
		r.setState(Shutdown)
	}
}
