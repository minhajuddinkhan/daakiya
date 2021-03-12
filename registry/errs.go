package registry

//ErrOffsetUnavailable ErrOffsetUnavailable
type ErrOffsetUnavailable struct {
	Message string
}

func (e *ErrOffsetUnavailable) Error() string {
	return e.Message
}
