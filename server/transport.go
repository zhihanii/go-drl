package server

import (
	"bufio"
	"context"
	"github.com/zhihanii/go-drl/pkg/bio"
	"github.com/zhihanii/go-drl/server/protocol"
	"log"
	"net"
	"time"
)

type Transport interface {
	Consumer() chan *protocol.Request
	Shutdown()
}

type NetworkTransport struct {
	ctx    context.Context
	cancel context.CancelFunc

	reqCh chan *protocol.Request

	listener net.Listener

	localAddr string

	timeout time.Duration

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
		reqCh:      make(chan *protocol.Request, 1),
		listener:   listener,
		localAddr:  localAddr,
		shutdownCh: make(chan struct{}),
		timeout:    time.Second * 5,
	}
	go t.listen()
	return t, nil
}

func (t *NetworkTransport) Consumer() chan *protocol.Request {
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
		header *protocol.RequestHeader
		resp   *protocol.RequestResponse
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

		header, err = protocol.ReadRequestHeader(r)
		if err != nil && err.Error() != "EOF" {
			return
		}

		resp = t.handle(r, *header)
		if resp.Error != nil {
			return
		}

		err = t.sendResp(wb, resp.Response)
		if err != nil {
			return
		}
	}
}

func (t *NetworkTransport) handle(r *bufio.Reader, header protocol.RequestHeader) *protocol.RequestResponse {
	var (
		respCh = make(chan *protocol.RequestResponse, 1)
		cmd    interface{}
	)

	switch protocol.ApiKey(header.ApiKey) {
	case protocol.Take:
		var req protocol.TakeRequest
		err := req.ReadFrom(r)
		if err != nil {
			return &protocol.RequestResponse{
				Response: nil,
				Error:    err,
			}
		}
		req.RequestHeader = header
		cmd = &req

	default:
		return &protocol.RequestResponse{
			Response: nil,
			Error:    protocol.ErrApiKey,
		}
	}

	t.reqCh <- &protocol.Request{
		Command:  cmd,
		RespChan: respCh,
	}

	select {
	case res := <-respCh:
		return res
	case <-t.shutdownCh:
		return &protocol.RequestResponse{
			Response: nil,
			Error:    protocol.ErrTransportShutdown,
		}
	}
}

func (t *NetworkTransport) sendResp(wb *bio.WriteBuffer, resp bio.Writable) error {
	//lock
	return resp.WriteTo(wb)
}
