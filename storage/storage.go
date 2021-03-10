package storage

//Storage a storage interface keeping queue
type Storage interface {
	//Append appends message to the queue
	Append(message []byte) error
	Flush()
	Get(offset uint64) ([]byte, error)
}
