package storage

import (
	"fmt"
	"sync"
)

type queue struct {
	offset   uint64
	messages map[uint64][]byte
}

func (q *queue) append(message []byte) {
	q.messages[q.offset] = message
	q.offset++
}

func (q *queue) get(offset uint64) ([]byte, error) {
	v, ok := q.messages[offset]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return v, nil
}

type kvStore struct {
	sync.Mutex
	queues map[string]queue
}

func NewKVStorage() Storage {
	return &kvStore{
		Mutex:  sync.Mutex{},
		queues: make(map[string]queue),
	}
}

func (s *kvStore) Append(hash string, message []byte) error {

	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	_, ok := s.queues[hash]
	if !ok {
		s.queues[hash] = queue{offset: 0, messages: make(map[uint64][]byte)}
	}
	q := s.queues[hash]
	q.append(message)
	s.queues[hash] = q

	return nil
}

func (s *kvStore) Flush(hash string) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	if _, ok := s.queues[hash]; !ok {
		return
	}
	s.queues[hash] = queue{offset: 0, messages: make(map[uint64][]byte)}
}

func (s *kvStore) Get(hash string, offset uint64) ([]byte, error) {

	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	q, ok := s.queues[hash]
	if !ok {
		return nil, &ErrHashNotFound{Message: fmt.Sprintf("cannot find hash at offset %s", hash)}
	}

	if offset > uint64(q.offset) {
		return nil, &OffsetUnavailable{Message: fmt.Sprintf("message not written on offset %d yet", offset)}
	}

	message, err := q.get(offset)
	if err != nil {
		return nil, &OffsetNotFound{message: fmt.Sprintf("cannot find error at offset %d", offset)}
	}

	return message, nil
}

func (s *kvStore) GetLastAvailableOffset(hash string) (uint, error) {
	v, ok := s.queues[hash]
	if !ok {
		return 0, &ErrHashNotFound{Message: fmt.Sprintf("cannot find hash: %s", hash)}
	}

	min := 0
	for k := range v.messages {
		if k < uint64(min) {
			min = int(k)
		}
	}

	return uint(min), nil
}
