package raft

import (
	"bufio"
	"go-drl/pkg/bio"
)

type ShutdownRequest struct {
	RequestHeader
}

func (t *ShutdownRequest) WriteTo(wb *bio.WriteBuffer) error {
	err := t.RequestHeader.WriteTo(wb)
	if err != nil {
		return err
	}
	return wb.Flush()
}

func (t *ShutdownRequest) ReadFrom(r *bufio.Reader) error {
	return nil
}

type ShutdownResponse struct {
	RequestHeader
	Success bool
}

func (t *ShutdownResponse) WriteTo(wb *bio.WriteBuffer) error {
	err := t.RequestHeader.WriteTo(wb)
	if err != nil {
		return err
	}
	err = wb.WriteBool(t.Success)
	if err != nil {
		return err
	}
	return wb.Flush()
}

func (t *ShutdownResponse) ReadFrom(r *bufio.Reader) error {
	var err error
	err = t.RequestHeader.ReadFrom(r)
	if err != nil {
		return err
	}
	t.Success, err = bio.ReadBool(r)
	return err
}
