package storage

//OffsetNotFound offset not found
type OffsetNotFound struct {
	message string
}

func (e *OffsetNotFound) Error() string {
	return e.message
}

//OffsetUnavailable error when offset is not yet written on disk
type OffsetUnavailable struct {
	Message string
}

func (e *OffsetUnavailable) Error() string {
	return e.Message
}

type ErrHashNotFound struct {
	Message string
}

func (e *ErrHashNotFound) Error() string {
	return e.Message

}
