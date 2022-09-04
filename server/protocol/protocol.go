package protocol

import (
	"bufio"
	"go-drl/pkg/bio"
)

type ApiKey int16

const (
	Take ApiKey = iota
)

type RequestResponse struct {
	Response bio.Writable
	Error    error
}

type Request struct {
	Command  interface{}
	RespChan chan *RequestResponse
}

func (r *Request) Respond(resp bio.Writable, err error) {
	r.RespChan <- &RequestResponse{Response: resp, Error: err}
}

type RequestHeader struct {
	ApiKey int16
}

func (h *RequestHeader) WriteTo(wb *bio.WriteBuffer) error {
	wb.WriteInt16(h.ApiKey)
	return wb.Flush()
}

func (h *RequestHeader) ReadFrom(r *bufio.Reader) error {
	var err error
	h.ApiKey, err = bio.ReadInt16(r)
	if err != nil {
		return err
	}
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
	if err != nil {
		return nil, err
	}
	return &header, nil
}
