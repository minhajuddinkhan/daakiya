package registry

import (
	"context"
	"fmt"

	"github.com/minhajuddinkhan/daakiya/storage"
)

func (r *registry) byNegativeOffset(ctx context.Context, query Query) (chan []byte, error) {

	ch := make(chan []byte)
	closeChannels := func(channels ...chan []byte) {
		for _, c := range channels {
			close(c)
		}
	}

	go func() {

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

				fmt.Println("QUERYIED OFFSET", sq.Offset)
				val, err := r.store.Get(sq)
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
				ch <- val
				offset++
			}
		}
	}()

	return ch, nil

}
