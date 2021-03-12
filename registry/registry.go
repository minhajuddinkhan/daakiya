package registry

import (
	"context"
	"sync"

	"github.com/minhajuddinkhan/daakiya/storage"
)

//Registry Registry
type Registry interface {
	Append(hash string, message []byte)
	FromOffset(context context.Context, hash string, offset int) (chan []byte, error)
	// NextMessageAvailable() chan struct{}
}
type registry struct {
	store         storage.Storage
	cond          *sync.Cond
	offsetFetcher Fetcher
}

func NewRegistry(store storage.Storage) Registry {
	return &registry{
		store:         store,
		cond:          sync.NewCond(&sync.Mutex{}),
		offsetFetcher: NewOffsetFetcher(store),
	}
}

//Append adds message in the store
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

	if offset < 0 {
		return r.byNegativeOffset(ctx, hash, offset)
	}

	return r.byPositiveOffset(ctx, hash, offset)

	// oldestOffset, latestOffset, err := r.getLimitingOffsets(hash)
	// noNewMessages := false
	// if err != nil {
	// 	switch err.(type) {

	// 	case *storage.ErrHashNotFound:
	// 		noNewMessages = true
	// 		break

	// 	default:
	// 		return nil, &ErrOffsetUnavailable{Message: err.Error()}

	// 	}
	// }

	// if !retryIfLastOffsetExpiresBetweenFetchTime {

	// }

	// if retryIfLastOffsetExpiresBetweenFetchTime && noNewMessages {
	// 	offset = 0
	// } else if retryIfLastOffsetExpiresBetweenFetchTime && !noNewMessages {

	// }
	// // return nil, &ErrOffsetUnavailable{Message: err.Error()}

	// // else {
	// // 	offset = int(oldestOffset)
	// // }

	// if offset >= 0 && !retryIfLastOffsetExpiresBetweenFetchTime {
	// 	if uint(offset) < oldestOffset {
	// 		return nil, &ErrOffsetUnavailable{Message: fmt.Sprintf("offset %d unavailable on disk", offset)}
	// 	}

	// 	if uint(offset) > latestOffset {
	// 		return nil, &ErrOffsetUnavailable{Message: fmt.Sprintf("offset %d unavailable on disk", offset)}
	// 	}
	// }

	// ch := make(chan []byte)

	// closeChannels := func(channels ...chan []byte) {
	// 	for _, c := range channels {
	// 		close(c)
	// 	}
	// }

	// go func() {
	// 	for {

	// 		select {
	// 		case <-ctx.Done():
	// 			closeChannels(ch)
	// 			return
	// 		default:

	// 			val, err := r.store.Get(hash, uint64(offset))
	// 			if err != nil {
	// 				switch err.(type) {
	// 				case *storage.OffsetUnavailable:
	// 					<-r.nextMessageAvailable()
	// 					continue

	// 				case *storage.OffsetNotFound:
	// 					if retryIfLastOffsetExpiresBetweenFetchTime {
	// 						offset++
	// 						continue
	// 					}
	// 					closeChannels(ch)
	// 					return

	// 				default:
	// 					closeChannels(ch)
	// 					return
	// 				}
	// 			}
	// 			ch <- val
	// 			offset++
	// 		}

	// 	}
	// }()
	// return ch, nil

}
