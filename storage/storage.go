package storage

//Storage a storage interface keeping queue
type Storage interface {
	//Append appends message to the queue
	Append(hash string, message []byte) error
	Flush(hash string)
	Get(hash string, offset uint64) ([]byte, error)
	GetOldestOffset(hash string) (uint, error)
	GetLatestOffset(hash string) (uint, error)
}
