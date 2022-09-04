package raft

import (
	"bufio"
	"github.com/zhihanii/go-drl/pkg/bio"
)

type RequestVoteRequest struct {
	RequestHeader
	Term               uint64
	LeadershipTransfer bool
}

func (t *RequestVoteRequest) WriteTo(wb *bio.WriteBuffer) error {
	err := t.RequestHeader.WriteTo(wb)
	if err != nil {
		return err
	}
	err = wb.WriteUint64(t.Term)
	if err != nil {
		return err
	}
	err = wb.WriteBool(t.LeadershipTransfer)
	if err != nil {
		return err
	}
	return wb.Flush()
}

func (t *RequestVoteRequest) ReadFrom(r *bufio.Reader) error {
	var err error
	t.Term, err = bio.ReadUint64(r)
	if err != nil {
		return err
	}
	t.LeadershipTransfer, err = bio.ReadBool(r)
	return err
}

type RequestVoteResponse struct {
	RequestHeader
	Term    uint64
	Peers   []string
	Granted bool
}

func (t *RequestVoteResponse) WriteTo(wb *bio.WriteBuffer) error {
	err := t.RequestHeader.WriteTo(wb)
	if err != nil {
		return err
	}
	err = wb.WriteUint64(t.Term)
	if err != nil {
		return err
	}
	err = wb.WriteSlice(len(t.Peers), func(i int) error {
		return wb.WriteString(t.Peers[i])
	})
	if err != nil {
		return err
	}
	err = wb.WriteBool(t.Granted)
	if err != nil {
		return err
	}
	return wb.Flush()
}

func (t *RequestVoteResponse) ReadFrom(r *bufio.Reader) error {
	var err error
	err = t.RequestHeader.ReadFrom(r)
	if err != nil {
		return err
	}
	t.Term, err = bio.ReadUint64(r)
	if err != nil {
		return err
	}
	t.Peers, err = bio.ReadSliceString(r)
	if err != nil {
		return err
	}
	t.Granted, err = bio.ReadBool(r)
	return err
}
