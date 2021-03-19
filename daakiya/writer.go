package daakiyaa

import (
	"time"

	"github.com/minhajuddinkhan/daakiya/storage"
)

//Registry Registry
type DaakiyaWriter interface {
	Write(m AppendMessage) error
}
type daakiyaW struct {
	store storage.StorageWriter
}

func NewDaakiyaWriter(store storage.StorageWriter) DaakiyaWriter {
	return &daakiyaW{store: store}
}

//Append adds message in the store
func (d *daakiyaW) Write(message AppendMessage) error {

	if err := message.Validate(); err != nil {
		return err
	}
	return d.store.Put(storage.Message{
		Topic:     message.Topic,
		Hash:      message.Hash,
		Value:     message.Value,
		Timestamp: message.Timestamp.Format(time.RFC1123),
	})
}
