package protocol

import (
	"bufio"
	"github.com/zhihanii/go-drl/pkg/bio"
)

type TakeRequest struct {
	RequestHeader
}

func (t *TakeRequest) WriteTo(wb *bio.WriteBuffer) error {
	err := t.RequestHeader.WriteTo(wb)
	if err != nil {
		return err
	}
	return wb.Flush()
}

func (t *TakeRequest) ReadFrom(r *bufio.Reader) error {
	return nil
}

type LeaderStateResponse struct {
	LeaderId   string
	LeaderAddr string
}

func (t *LeaderStateResponse) WriteTo(wb *bio.WriteBuffer) error {
	err := wb.WriteString(t.LeaderId)
	if err != nil {
		return err
	}
	return wb.WriteString(t.LeaderAddr)
}

func (t *LeaderStateResponse) ReadFrom(r *bufio.Reader) error {
	var err error
	t.LeaderId, err = bio.ReadString(r)
	if err != nil {
		return err
	}
	t.LeaderAddr, err = bio.ReadString(r)
	return err
}

type TakeResponse struct {
	Success bool
	LeaderStateResponse
	Tokens int64
}

func (t *TakeResponse) WriteTo(wb *bio.WriteBuffer) error {
	err := wb.WriteBool(t.Success)
	if err != nil {
		return err
	}
	err = t.LeaderStateResponse.WriteTo(wb)
	if err != nil {
		return err
	}
	err = wb.WriteInt64(t.Tokens)
	if err != nil {
		return err
	}
	return wb.Flush()
}

func (t *TakeResponse) ReadFrom(r *bufio.Reader) error {
	var err error
	t.Success, err = bio.ReadBool(r)
	if err != nil {
		return err
	}
	err = t.LeaderStateResponse.ReadFrom(r)
	if err != nil {
		return err
	}
	t.Tokens, err = bio.ReadInt64(r)
	return err
}
