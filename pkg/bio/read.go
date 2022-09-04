package bio

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
)

type Readable interface {
	ReadFrom(*bufio.Reader) error
}

var errShortRead = errors.New("not enough bytes available to load the response")

func ReadInt16(r *bufio.Reader) (int16, error) {
	b := make([]byte, 2)
	_, err := io.ReadFull(r, b)
	if err != nil {
		return 0, err
	}
	return int16(binary.BigEndian.Uint16(b)), nil
}

func ReadInt32(r *bufio.Reader) (int32, error) {
	b := make([]byte, 4)
	_, err := io.ReadFull(r, b)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(b)), nil
}

func ReadInt64(r *bufio.Reader) (int64, error) {
	b := make([]byte, 8)
	_, err := io.ReadFull(r, b)
	if err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(b)), nil
}

func ReadUint64(r *bufio.Reader) (uint64, error) {
	b := make([]byte, 8)
	_, err := io.ReadFull(r, b)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b), nil
}

func ReadBool(r *bufio.Reader) (bool, error) {
	i, err := ReadInt16(r)
	if err != nil {
		return false, err
	}
	if i == 1 {
		return true, nil
	} else {
		return false, nil
	}
}

func ReadSliceInt32(r *bufio.Reader) ([]int32, error) {
	n, err := ReadInt32(r)
	if err != nil {
		return nil, err
	}
	res := make([]int32, n)
	var v int32
	for i := 0; i < int(n); i++ {
		v, err = ReadInt32(r)
		if err != nil {
			return nil, err
		}
		res = append(res, v)
	}
	return res, nil
}

func ReadString(r *bufio.Reader) (string, error) {
	n, err := ReadInt16(r)
	if err != nil {
		return "", err
	}
	b := make([]byte, n)
	_, err = io.ReadFull(r, b)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func ReadSliceString(r *bufio.Reader) ([]string, error) {
	n, err := ReadInt32(r)
	if err != nil {
		return nil, err
	}
	res := make([]string, n)
	var s string
	for i := 0; i < int(n); i++ {
		s, err = ReadString(r)
		if err != nil {
			return nil, err
		}
		res = append(res, s)
	}
	return res, nil
}

func ReadMapStringSliceInt32(r *bufio.Reader) (map[string][]int32, error) {
	var (
		res = make(map[string][]int32)
		n   int32
		k   string
		v   []int32
		err error
	)
	n, err = ReadInt32(r)
	if err != nil {
		return nil, err
	}
	for i := 0; i < int(n); i++ {
		k, err = ReadString(r)
		if err != nil {
			return nil, err
		}
		v, err = ReadSliceInt32(r)
		if err != nil {
			return nil, err
		}
		res[k] = v
	}
	return res, nil
}
