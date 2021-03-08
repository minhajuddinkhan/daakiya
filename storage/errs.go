package storage

//OffsetNotFound offset not found
type OffsetNotFound struct {
	message string
}

func (e *OffsetNotFound) Error() string {
	return e.message
}
