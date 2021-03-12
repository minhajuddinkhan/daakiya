package storage

//OffsetNotFound offset not found
type OffsetNotFound struct {
	Message string
}

func (e *OffsetNotFound) Error() string {
	return e.Message
}

type ErrHashNotFound struct {
	Message string
}

func (e *ErrHashNotFound) Error() string {
	return e.Message

}
