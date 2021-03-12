package registry

//ErrOffsetUnavailable ErrOffsetUnavailable
type ErrOffsetUnavailable struct {
	Message string
}

func (e *ErrOffsetUnavailable) Error() string {
	return e.Message
}

type ErrInvalidMessage struct {
	Message string
}

func (e *ErrInvalidMessage) Error() string {
	return e.Message
}
