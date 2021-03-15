package daakiyaa

import (
	"context"

	"github.com/minhajuddinkhan/daakiya/storage"
)

func (r *daakiya) byPositiveOffset(ctx context.Context, query Query) (chan []byte, error) {

	offset, err := r.offsetFetcher.Fetch(query)
	if err != nil {
		return nil, err
	}

	ch := make(chan []byte)
	closeChannels := func(channels ...chan []byte) {
		for _, c := range channels {
			close(c)
		}
		return
	}

	go func() {

		for {
			select {
			case <-ctx.Done():
				closeChannels(ch)
				return

			default:

				sq := storage.Query{
					Topic:  query.Topic,
					Hash:   query.Hash,
					Offset: uint64(offset),
				}

				message, err := r.store.Get(sq)
				if err != nil {
					switch err.(type) {
					case *storage.ErrHashNotFound:
						<-r.nextMessageAvailable()
						continue

					case *storage.OffsetNotFound:
						<-r.nextMessageAvailable()
						continue

					default:
						closeChannels(ch)
						return
					}
				}

				ch <- message.Value
				offset++
			}

		}
	}()

	return ch, nil
}
