package daakiyaa

import (
	"context"
	"sync"

	"github.com/minhajuddinkhan/daakiya/storage"
)

//Registry Registry
type Daakiya interface {
	Append(m AppendMessage) error
	FromOffset(context context.Context, q Query) (chan []byte, error)
}
type daakiya struct {
	store         storage.Storage
	cond          *sync.Cond
	offsetFetcher Fetcher
}

func NewDaakiya(store storage.Storage) Daakiya {
	return &daakiya{
		store:         store,
		cond:          sync.NewCond(&sync.Mutex{}),
		offsetFetcher: NewOffsetFetcher(store),
	}
}

//Append adds message in the store
func (r *daakiya) Append(message AppendMessage) error {

	if err := message.Validate(); err != nil {
		return err
	}

	r.cond.L.Lock()
	defer func() {
		r.cond.Broadcast()
		r.cond.L.Unlock()

	}()

	return r.store.Append(storage.Message{
		Topic: message.Topic,
		Hash:  message.Hash,
		Value: message.Value,
	})
}

//FromOffset returns a channel that provides all available messages
func (r *daakiya) FromOffset(ctx context.Context, q Query) (chan []byte, error) {
	if q.Offset < 0 {
		return r.byNegativeOffset(ctx, q)
	}
	return r.byPositiveOffset(ctx, q)
}

func (r *daakiya) nextMessageAvailable() chan struct{} {

	c := make(chan struct{})
	r.cond.L.Lock()

	r.cond.Wait()
	go func(channel chan struct{}) {
		channel <- struct{}{}
	}(c)
	r.cond.L.Unlock()
	return c
}
