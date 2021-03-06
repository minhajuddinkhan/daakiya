package daakiyaa

import "time"

type AppendMessage struct {
	Topic     string
	Hash      string
	Value     []byte
	Timestamp time.Time
}

func (m *AppendMessage) Validate() error {

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
