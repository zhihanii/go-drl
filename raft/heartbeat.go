package raft

import (
	"bufio"
	"github.com/zhihanii/go-drl/pkg/bio"
)

type HeartbeatRequest struct {
	RequestHeader
	Term uint64
}

func (t *HeartbeatRequest) WriteTo(wb *bio.WriteBuffer) error {
	err := t.RequestHeader.WriteTo(wb)
	if err != nil {
		return err
	}
	err = wb.WriteUint64(t.Term)
	if err != nil {
		return err
	}
	return wb.Flush()
}

func (t *HeartbeatRequest) ReadFrom(r *bufio.Reader) error {
	var err error
	t.Term, err = bio.ReadUint64(r)
	return err
}

type HeartbeatResponse struct {
	RequestHeader
	Term    uint64
	Success bool
}

func (t *HeartbeatResponse) WriteTo(wb *bio.WriteBuffer) error {
	err := t.RequestHeader.WriteTo(wb)
	if err != nil {
		return err
	}
	err = wb.WriteUint64(t.Term)
	if err != nil {
		return err
	}
	err = wb.WriteBool(t.Success)
	if err != nil {
		return err
	}
	return wb.Flush()
}

func (t *HeartbeatResponse) ReadFrom(r *bufio.Reader) error {
	var err error
	err = t.RequestHeader.ReadFrom(r)
	if err != nil {
		return err
	}
	t.Term, err = bio.ReadUint64(r)
	if err != nil {
		return err
	}
	t.Success, err = bio.ReadBool(r)
	return err
}
