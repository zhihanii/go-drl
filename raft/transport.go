package raft

import (
	"bufio"
	"context"
	"errors"
	"go-drl/pkg/bio"
	"log"
	"net"
	"sync"
	"time"
)

type Transport interface {
	Consumer() chan *Request
	LocalAddr() string
	Heartbeat(id string, target string, req *HeartbeatRequest, resp *HeartbeatResponse) error
	RequestVote(id string, target string, req *RequestVoteRequest, resp *RequestVoteResponse) error
	ConfigChange(id string, target string, req *ConfigChangeRequest, resp *ConfigChangeResponse) error
	ShutdownRequest(id string, target string, req *ShutdownRequest, resp *ShutdownResponse) error
	SetHeartbeatHandler(cb func(req *Request))
	Shutdown()
}

type NetworkTransport struct {
	ctx    context.Context
	cancel context.CancelFunc

	reqCh chan *Request

	connPoolLock sync.Mutex
	connPool     map[string][]*netConn

	maxPool int

	listener net.Listener

	localAddr string

	timeout time.Duration

	heartbeatFnLock sync.Mutex
	heartbeatFn     func(req *Request)

	shutdownCh chan struct{}
}

type netConn struct {
	target string
	conn   net.Conn
	r      *bufio.Reader
	w      *bufio.Writer
	wb     *bio.WriteBuffer
}

func (n *netConn) Release() error {
	return n.conn.Close()
}

func NewNetworkTransport(localAddr string) (*NetworkTransport, error) {
	ctx, cancel := context.WithCancel(context.Background())
	listener, err := net.Listen("tcp", localAddr)
	if err != nil {
		cancel()
		return nil, err
	}
	t := &NetworkTransport{
		ctx:        ctx,
		cancel:     cancel,
		reqCh:      make(chan *Request, 1),
		connPool:   make(map[string][]*netConn),
		maxPool:    30,
		listener:   listener,
		localAddr:  localAddr,
		shutdownCh: make(chan struct{}),
		timeout:    time.Second * 5,
	}
	go t.listen()
	return t, nil
}

func (t *NetworkTransport) Consumer() chan *Request {
	return t.reqCh
}

func (t *NetworkTransport) LocalAddr() string {
	return t.localAddr
}

func (t *NetworkTransport) IsShutdown() bool {
	select {
	case <-t.shutdownCh:
		return true
	default:
		return false
	}
}

func (t *NetworkTransport) Shutdown() {
	t.cancel()
	close(t.shutdownCh)
}

func (t *NetworkTransport) SetHeartbeatHandler(cb func(req *Request)) {
	t.heartbeatFnLock.Lock()
	t.heartbeatFn = cb
	t.heartbeatFnLock.Unlock()
}

func (t *NetworkTransport) getConn(target string) (*netConn, error) {
	if conn := t.getPooledConn(target); conn != nil {
		return conn, nil
	}
	conn, err := net.DialTimeout("tcp", target, t.timeout)
	if err != nil {
		return nil, err
	}
	nc := &netConn{
		target: target,
		conn:   conn,
		r:      bufio.NewReader(conn),
		w:      bufio.NewWriter(conn),
	}
	nc.wb = bio.NewWriteBuffer(nc.w)
	return nc, nil
}

func (t *NetworkTransport) getPooledConn(target string) *netConn {
	t.connPoolLock.Lock()
	defer t.connPoolLock.Unlock()

	conns, ok := t.connPool[target]
	if !ok || len(conns) == 0 {
		return nil
	}

	var conn *netConn
	num := len(conns)
	conn, conns[num-1] = conns[num-1], nil
	t.connPool[target] = conns[:num-1]
	return conn
}

func (t *NetworkTransport) returnConn(conn *netConn) {
	t.connPoolLock.Lock()
	defer t.connPoolLock.Unlock()

	key := conn.target
	conns, _ := t.connPool[key]

	if !t.IsShutdown() && len(conns) < t.maxPool {
		t.connPool[key] = append(conns, conn)
	} else {
		conn.Release()
	}
}

func (t *NetworkTransport) listen() {

	for {
		conn, err := t.listener.Accept()
		if err != nil {
			log.Printf("address:%s listen failed, err:%v", t.localAddr, err)
			return
		}
		go t.handleConn(t.ctx, conn)
	}
}

func (t *NetworkTransport) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	var (
		header *RequestHeader
		resp   *RequestResponse
		err    error
	)

	r := bufio.NewReaderSize(conn, bio.DefaultBufferSize)
	wb := bio.NewWriteBuffer(bufio.NewWriterSize(conn, bio.DefaultBufferSize))

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		header, err = ReadRequestHeader(r)
		if err != nil && err.Error() != "EOF" {
			log.Printf("address:%s read request header failed, err:%v", t.localAddr, err)
			return
		}

		resp = t.handle(r, *header)
		if resp.Error != nil {
			log.Printf("address:%s handle request, err:%v", t.localAddr, resp.Error)
			return
		}

		err = t.sendResp(wb, resp.Response)
		if err != nil {
			log.Printf("address:%s send resp, err:%v", t.localAddr, err)
			return
		}
	}
}

func (t *NetworkTransport) handle(r *bufio.Reader, header RequestHeader) *RequestResponse {
	var (
		cmd    interface{}
		respCh = make(chan *RequestResponse, 1)
	)

	switch ApiKey(header.ApiKey) {
	case Heartbeat:
		var req HeartbeatRequest
		err := req.ReadFrom(r)
		if err != nil {
			return &RequestResponse{
				Response: nil,
				Error:    err,
			}
		}
		req.RequestHeader = header

		t.heartbeatFnLock.Lock()
		fn := t.heartbeatFn
		t.heartbeatFnLock.Unlock()
		if fn != nil {
			fn(&Request{
				Command:  &req,
				RespChan: respCh,
			})
			goto RESP
		} else {
			cmd = &req
		}

	case RequestVote:
		var req RequestVoteRequest
		err := req.ReadFrom(r)
		if err != nil {
			return &RequestResponse{
				Response: nil,
				Error:    err,
			}
		}
		req.RequestHeader = header
		cmd = &req

	case ConfigChange:
		var req ConfigChangeRequest
		err := req.ReadFrom(r)
		if err != nil {
			return &RequestResponse{
				Response: nil,
				Error:    err,
			}
		}
		req.RequestHeader = header
		cmd = &req

	case RequestShutdown:
		var req ShutdownRequest
		err := req.ReadFrom(r)
		if err != nil {
			return &RequestResponse{
				Response: nil,
				Error:    err,
			}
		}
		req.RequestHeader = header
		cmd = &req
	}

	t.reqCh <- &Request{
		Command:  cmd,
		RespChan: respCh,
	}

RESP:
	select {
	case res := <-respCh:
		return res
	case <-t.shutdownCh:
		resp := RequestResponse{
			Response: nil,
			Error:    errors.New("transport shutdown"),
		}
		return &resp
	}
}

func (t *NetworkTransport) sendResp(wb *bio.WriteBuffer, resp bio.Writable) error {
	//lock
	return resp.WriteTo(wb)
}

func (t *NetworkTransport) roundTrip(id string, target string, req bio.Writable, resp bio.Readable) error {
	var err error

	conn, err := t.getConn(target)
	if err != nil {
		return err
	}

	if t.timeout > 0 {
		conn.conn.SetDeadline(time.Now().Add(t.timeout))
	}

	err = t.sendReq(conn.wb, req)
	if err != nil {
		return err
	}

	err = resp.ReadFrom(conn.r)
	if err != nil {
		return err
	}

	t.returnConn(conn)
	return nil
}

func (t *NetworkTransport) sendReq(wb *bio.WriteBuffer, req bio.Writable) error {
	return req.WriteTo(wb)
}

func (t *NetworkTransport) Heartbeat(id string, target string, req *HeartbeatRequest, resp *HeartbeatResponse) error {
	return t.roundTrip(id, target, req, resp)
}

func (t *NetworkTransport) RequestVote(id string, target string, req *RequestVoteRequest, resp *RequestVoteResponse) error {
	return t.roundTrip(id, target, req, resp)
}

func (t *NetworkTransport) ConfigChange(id string, target string, req *ConfigChangeRequest, resp *ConfigChangeResponse) error {
	return t.roundTrip(id, target, req, resp)
}

func (t *NetworkTransport) ShutdownRequest(id string, target string, req *ShutdownRequest, resp *ShutdownResponse) error {
	return t.roundTrip(id, target, req, resp)
}
