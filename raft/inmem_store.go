package raft

import (
	"errors"
	"sync"
)

type InmemStore struct {
	l         sync.RWMutex
	lowIndex  uint64
	highIndex uint64
	kv        map[string][]byte
	kvInt     map[string]uint64
}

func NewInmemStore() *InmemStore {
	i := &InmemStore{
		kv:    make(map[string][]byte),
		kvInt: make(map[string]uint64),
	}
	return i
}

func (i *InmemStore) FirstIndex() (uint64, error) {
	i.l.RLock()
	defer i.l.RUnlock()
	return i.lowIndex, nil
}

func (i *InmemStore) LastIndex() (uint64, error) {
	i.l.RLock()
	defer i.l.RUnlock()
	return i.highIndex, nil
}

func (i *InmemStore) Set(key []byte, val []byte) error {
	i.l.Lock()
	defer i.l.Unlock()
	i.kv[string(key)] = val
	return nil
}

func (i *InmemStore) Get(key []byte) ([]byte, error) {
	i.l.RLock()
	defer i.l.RUnlock()
	val := i.kv[string(key)]
	if val == nil {
		return nil, errors.New("not found")
	}
	return val, nil
}

func (i *InmemStore) SetUint64(key []byte, val uint64) error {
	i.l.Lock()
	defer i.l.Unlock()
	i.kvInt[string(key)] = val
	return nil
}

func (i *InmemStore) GetUint64(key []byte) (uint64, error) {
	i.l.RLock()
	defer i.l.RUnlock()
	return i.kvInt[string(key)], nil
}
