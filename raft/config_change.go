package raft

import (
	"bufio"
	"go-drl/pkg/bio"
)

type ConfigChangeRequest struct {
	RequestHeader
	Configuration Configuration
}

func (t *ConfigChangeRequest) WriteTo(wb *bio.WriteBuffer) error {
	err := t.RequestHeader.WriteTo(wb)
	if err != nil {
		return err
	}
	err = t.Configuration.WriteTo(wb)
	if err != nil {
		return err
	}
	return wb.Flush()
}

func (t *ConfigChangeRequest) ReadFrom(r *bufio.Reader) error {
	return t.Configuration.ReadFrom(r)
}

type ConfigChangeResponse struct {
	RequestHeader
	Success bool
}

func (t *ConfigChangeResponse) WriteTo(wb *bio.WriteBuffer) error {
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

func (t *ConfigChangeResponse) ReadFrom(r *bufio.Reader) error {
	var err error
	err = t.RequestHeader.ReadFrom(r)
	if err != nil {
		return err
	}
	t.Success, err = bio.ReadBool(r)
	return err
}
