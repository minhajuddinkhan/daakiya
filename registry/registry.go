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
}
