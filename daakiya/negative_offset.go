package daakiyaa

import (
	"context"

	"github.com/minhajuddinkhan/daakiya/storage"
)

func (r *daakiya) byNegativeOffset(ctx context.Context, query Query) (chan []byte, error) {

	ch := make(chan []byte)
	closeChannels := func(channels ...chan []byte) {
		for _, c := range channels {
			close(c)
		}
	}

	go func(c Courier) {

		var offset uint

		waiting := true

		for waiting {
			select {
			case <-ctx.Done():
				closeChannels(ch)
				return

			default:

				o, err := r.offsetFetcher.Fetch(query)
				if err != nil {
					switch err.(type) {
					case *storage.ErrHashNotFound:
						<-r.nextMessageAvailable()
						continue
					default:
						closeChannels(ch)
						return
					}
				}

				offset = o
				waiting = false
				break
			}

		}

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

					case *storage.OffsetNotFound:
						<-r.nextMessageAvailable()
						continue

					default:

						closeChannels(ch)
						return
					}
				}

				err = c.Deliver(*message)
				if err != nil {
					closeChannels(ch)
					return
				}
				// ch <- val
				offset++
			}
		}
	}(r.courier)

	return ch, nil

}
