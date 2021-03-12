package registry

import (
	"context"
	"fmt"
	"sync"

	"github.com/minhajuddinkhan/daakiya/storage"
)

//Registry Registry
type Registry interface {
	Append(hash string, message []byte)
	FromOffset(context context.Context, hash string, offset int) (chan []byte, error)
}
type registry struct {
	store storage.Storage
	cond  *sync.Cond
}

func NewRegistry(store storage.Storage) Registry {
	return &registry{
		store: store,
		cond:  sync.NewCond(&sync.Mutex{}),
	}
}

//Append Append
func (r *registry) Append(hash string, message []byte) {
	r.cond.L.Lock()
	r.cond.Broadcast()
	r.store.Append(hash, message)
	r.cond.L.Unlock()
}

func (r *registry) nextMessageAvailable() chan struct{} {

	c := make(chan struct{})
	r.cond.L.Lock()

	r.cond.Wait()
	go func(channel chan struct{}) {
		channel <- struct{}{}
	}(c)
	r.cond.L.Unlock()
	return c
}

//FromOffset returns a channel that provides all available messages
func (r *registry) FromOffset(ctx context.Context, hash string, offset int) (chan []byte, error) {

	retryIfLastOffsetExpiresBetweenFetchTime := false
	oldestOffset, err := r.store.GetLastAvailableOffset(hash)
	if err != nil {
		return nil, &ErrOffsetUnavailable{Message: err.Error()}
	}

	if offset >= 0 {
		if uint(offset) <= oldestOffset {
			return nil, &ErrOffsetUnavailable{Message: fmt.Sprintf("offset %d unavailable on disk", offset)}
		}
	}

	if offset < 0 {
		offset = int(oldestOffset)
		retryIfLastOffsetExpiresBetweenFetchTime = true
	}

	ch := make(chan []byte)

	closeChannels := func(channels ...chan []byte) {
		fmt.Println("closing channels.")
		for _, c := range channels {
			close(c)
		}
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
					case *storage.OffsetUnavailable:
						<-r.nextMessageAvailable()
						continue

					case *storage.OffsetNotFound:
						if retryIfLastOffsetExpiresBetweenFetchTime {
							offset++
							continue
						}
						closeChannels(ch)
						return

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
