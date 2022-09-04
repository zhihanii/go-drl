package client

import (
	"bufio"
	"go-drl/pkg/bio"
	"go-drl/server/protocol"
	"net"
)

type Transport interface {
	Take(req *protocol.TakeRequest, resp *protocol.TakeResponse) error
}

type NetworkTransport struct {
	l *RateLimiter

	conn *netConn
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

func NewNetworkTransport(l *RateLimiter) *NetworkTransport {
	t := &NetworkTransport{
		l: l,
	}
	err := t.connect()
	panic(err)
	return t
}

func (t *NetworkTransport) connect() error {
	c, err := net.Dial("tcp", t.l.state.LeaderAddr)
	if err != nil {
		return err
	}

	nc := &netConn{
		target: t.l.state.LeaderAddr,
		conn:   c,
		r:      bufio.NewReader(c),
		w:      bufio.NewWriter(c),
	}
	nc.wb = bio.NewWriteBuffer(nc.w)

	t.conn = nc
	return nil
}

func (t *NetworkTransport) reConnect() error {
	err := t.conn.Release()
	if err != nil {
		return err
	}
	return t.connect()
}

func (t *NetworkTransport) roundTrip(req bio.Writable, resp bio.Readable) error {
	var err error

	err = t.sendReq(t.conn.wb, req)
	if err != nil {
		err = t.reConnect()
		if err != nil {
			t.conn.Release()
			return err
		}
	}

	err = resp.ReadFrom(t.conn.r)
	if err != nil {

	}

	return nil
}

func (t *NetworkTransport) sendReq(wb *bio.WriteBuffer, req bio.Writable) error {
	return req.WriteTo(wb)
}

func (t *NetworkTransport) Take(req *protocol.TakeRequest, resp *protocol.TakeResponse) error {
	err := t.roundTrip(req, resp)
	if err != nil {
		return err
	}
	if !resp.Success {
		state := &leaderState{
			LeaderId: resp.LeaderId,
			LeaderAddr: resp.LeaderAddr,
		}
		t.l.setLeaderState(state)

		err = t.reConnect()
		if err != nil {
			return err
		}

		return t.roundTrip(req, resp)
	}
	return nil
}
