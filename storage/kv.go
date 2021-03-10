package storage

import (
	"fmt"
	"sync"
)

type kvStore struct {
	sync.Mutex
	offset   uint64
	messages map[uint64][]byte
}

func NewKVStorage() Storage {
	return &kvStore{
		Mutex:    sync.Mutex{},
		offset:   0,
		messages: make(map[uint64][]byte),
	}
}

func (s *kvStore) Append(message []byte) error {

	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	s.messages[s.offset] = message
	s.offset++
	return nil
}

func (s *kvStore) Flush() {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	s.messages = make(map[uint64][]byte)
}

func (s *kvStore) Get(offset uint64) ([]byte, error) {

	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	v, ok := s.messages[offset]
	if !ok {
		return nil, &OffsetNotFound{message: fmt.Sprintf("cannot find error at offset %d", offset)}
	}
	return v, nil
}

func (s *kvStore) MemSize() uint16 {
	return uint16(len(s.messages))
}

func (s *kvStore) LastOffset() uint64 {
	return s.offset
}
