package raft

import (
	"bufio"
	"go-drl/pkg/bio"
)

type ApiKey int16

const (
	Heartbeat ApiKey = iota
	RequestVote
	ConfigChange
	RequestShutdown
)

type RequestResponse struct {
	Response bio.Writable
	Error    error
}

type Request struct {
	Command  interface{}
	RespChan chan *RequestResponse
}

func (r *Request) respond(resp bio.Writable, err error) {
	r.RespChan <- &RequestResponse{Response: resp, Error: err}
}

type RequestHeader struct {
	ApiKey int16
	Id     string
	Addr   string
}

func (h *RequestHeader) WriteTo(wb *bio.WriteBuffer) error {
	err := wb.WriteInt16(h.ApiKey)
	if err != nil {
		return err
	}
	err = wb.WriteString(h.Id)
	if err != nil {
		return err
	}
	err = wb.WriteString(h.Addr)
	if err != nil {
		return err
	}
	return wb.Flush()
}

func (h *RequestHeader) ReadFrom(r *bufio.Reader) error {
	var err error
	h.ApiKey, err = bio.ReadInt16(r)
	if err != nil {
		return err
	}
	h.Id, err = bio.ReadString(r)
	if err != nil {
		return err
	}
	h.Addr, err = bio.ReadString(r)
	return err
}

func ReadRequestHeader(r *bufio.Reader) (*RequestHeader, error) {
	var (
		header RequestHeader
		err    error
	)
	header.ApiKey, err = bio.ReadInt16(r)
	if err != nil {
		return nil, err
	}
	header.Id, err = bio.ReadString(r)
	if err != nil {
		return nil, err
	}
	header.Addr, err = bio.ReadString(r)
	if err != nil {
		return nil, err
	}
	return &header, nil
}
