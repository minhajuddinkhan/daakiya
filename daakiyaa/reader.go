package daakiyaa

import (
	"context"

	"github.com/minhajuddinkhan/daakiya/storage"
)

type DaakiyaReader interface {
	Read(ctx context.Context, hash, topic string, offset uint) chan storage.Message
}

func NewDaakiyaReader(reader storage.StorageReader) DaakiyaReader {
	return &daakiyaReader{
		reader: reader,
	}
}

type daakiyaReader struct {
	reader storage.StorageReader
}

// //FromOffset returns a channel that provides all available messages
func (r *daakiyaReader) Read(ctx context.Context, hash, topic string, offset uint) chan storage.Message {
	return r.reader.ReadFrom(ctx, storage.Query{
		Topic:  topic,
		Hash:   hash,
		Offset: uint64(offset),
	})

}
