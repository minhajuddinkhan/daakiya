package registry

import (
	"context"

	"github.com/minhajuddinkhan/daakiya/storage"
)

func (r *registry) byPositiveOffset(ctx context.Context, hash string, offsetArg int) (chan []byte, error) {

	offset, err := r.offsetFetcher.Fetch(hash, offsetArg)
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

				val, err := r.store.Get(hash, uint64(offset))
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
				ch <- val
				offset++
			}

		}
	}()

	return ch, nil
}
