package registry

type Message struct {
	Topic  string
	Hash   string
	Offset uint
	Value  []byte
}

func (m *Message) Validate() error {

	if m.Topic == "" {
		return &ErrInvalidMessage{Message: "Topic cannot be empty"}
	}

	if m.Hash == "" {
		return &ErrInvalidMessage{Message: "Hash cannot be empty"}
	}

	if m.Value == nil {
		return &ErrInvalidMessage{Message: "Value cannot be empty"}
	}

	return nil
}
