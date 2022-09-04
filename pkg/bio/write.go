package bio

import (
	"bufio"
	"encoding/binary"
	"io"
)

const DefaultBufferSize = 16 * 1024

type Writable interface {
	WriteTo(*WriteBuffer) error
}

type WriteBuffer struct {
	//w bio.Writer
	w *bufio.Writer
	b [16]byte
}

func NewWriteBuffer(w *bufio.Writer) *WriteBuffer {
	return &WriteBuffer{w: w}
}

func (wb *WriteBuffer) Flush() error {
	return wb.w.Flush()
}

func (wb *WriteBuffer) write(b []byte) error {
	_, err := wb.w.Write(b)
	return err
}

func (wb *WriteBuffer) writeString(s string) error {
	_, err := io.WriteString(wb.w, s)
	return err
}

func (wb *WriteBuffer) WriteInt16(i int16) error {
	binary.BigEndian.PutUint16(wb.b[:2], uint16(i))
	return wb.write(wb.b[:2])
}

func (wb *WriteBuffer) WriteInt32(i int32) error {
	binary.BigEndian.PutUint32(wb.b[:4], uint32(i))
	return wb.write(wb.b[:4])
}

func (wb *WriteBuffer) WriteInt64(i int64) error {
	binary.BigEndian.PutUint64(wb.b[:8], uint64(i))
	return wb.write(wb.b[:8])
}

func (wb *WriteBuffer) WriteUint64(i uint64) error {
	binary.BigEndian.PutUint64(wb.b[:8], i)
	return wb.write(wb.b[:8])
}

func (wb *WriteBuffer) WriteBool(b bool) error {
	var i int16
	if b {
		i = 1
	} else {
		i = 0
	}
	return wb.WriteInt16(i)
}

func (wb *WriteBuffer) WriteString(s string) error {
	err := wb.WriteInt16(int16(len(s)))
	if err != nil {
		return err
	}
	return wb.writeString(s)
}

func (wb *WriteBuffer) WriteBytes(b []byte) error {
	n := len(b)
	if b == nil {
		n = 0
	}
	err := wb.WriteInt32(int32(n))
	if err != nil {
		return err
	}
	return wb.write(b)
}

func (wb *WriteBuffer) WriteSliceLen(n int) error {
	return wb.WriteInt32(int32(n))
}

func (wb *WriteBuffer) WriteSlice(n int, f func(int) error) error {
	err := wb.WriteSliceLen(n)
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		err = f(i)
		if err != nil {
			return err
		}
	}
	return nil
}

func (wb *WriteBuffer) WriteSliceInt32(a []int32) error {
	return wb.WriteSlice(len(a), func(i int) error {
		return wb.WriteInt32(a[i])
	})
}

func (wb *WriteBuffer) WriteMapStringSliceInt32(m map[string][]int32) error {
	err := wb.WriteSliceLen(len(m))
	if err != nil {
		return err
	}
	for k, v := range m {
		err = wb.WriteString(k)
		if err != nil {
			return err
		}
		err = wb.WriteSliceInt32(v)
		if err != nil {
			return err
		}
	}
	return nil
}
