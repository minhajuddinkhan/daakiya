package storage

//Storage a storage interface keeping queue
type Storage interface {
	Get(q Query) (*Message, error)
	Put(m Message) error
	Flush(hash string, topic string)

	GetOldestOffset(hash, topic string) (uint, error)
	GetLatestOffset(hash, topic string) (uint, error)
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
