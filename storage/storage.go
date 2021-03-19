package storage

import "context"

type Storage interface {
	StorageWriter
	StorageReader
}

type StorageWriter interface {
	Put(m Message) error
}

type StorageReader interface {
	ReadFrom(ctx context.Context, q Query) chan Message
}

type Message struct {
	Topic     string `json:"topic"`
	Hash      string `json:"hash"`
	Value     []byte `json:"value"`
	Offset    uint   `json:"offset"`
	Timestamp string `json:"timestamp"`
}

type Query struct {
	Topic  string
	Hash   string
	Offset uint64
}
