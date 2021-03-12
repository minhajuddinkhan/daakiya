package storage

//Storage a storage interface keeping queue
type Storage interface {
	//Append appends message to the queue
	Append(m Message) error
	Flush(hash string, topic string)
	Get(q Query) ([]byte, error)
	GetOldestOffset(hash, topic string) (uint, error)
	GetLatestOffset(hash, topic string) (uint, error)
}

type Message struct {
	Topic  string
	Hash   string
	Value  []byte
	Offset uint
}

type Query struct {
	Topic  string
	Hash   string
	Offset uint64
}
