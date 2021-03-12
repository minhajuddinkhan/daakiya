package registry

import (
	"context"
	"sync"

	"github.com/minhajuddinkhan/daakiya/storage"
)

//Registry Registry
type Registry interface {
	Append(m Message) error
	FromOffset(context context.Context, q Query) (chan []byte, error)
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
func (r *registry) Append(message Message) error {

	r.cond.L.Lock()
	defer r.cond.L.Unlock()
	r.cond.Broadcast()

	return r.store.Append(storage.Message{
		Topic: message.Topic,
		Hash:  message.Hash,
		Value: message.Value,
	})
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
func (r *registry) FromOffset(ctx context.Context, q Query) (chan []byte, error) {
	if q.Offset < 0 {
		return r.byNegativeOffset(ctx, q)
	}
	return r.byPositiveOffset(ctx, q)
}
