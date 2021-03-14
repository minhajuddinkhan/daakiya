package daakiyaa

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/minhajuddinkhan/daakiya/storage"
)

//Registry Registry
type Daakiya interface {
	Append(m AppendMessage) error
	FromOffset(context context.Context, q Query) (chan []byte, error)
}
type daakiya struct {
	store         storage.Storage
	mutex         sync.Mutex
	cond          *sync.Cond
	offsetFetcher Fetcher
	latestOffsets map[string]uint
	synchronized  map[string]bool
}

func NewDaakiya(store storage.Storage) Daakiya {

	d := &daakiya{
		store:         store,
		mutex:         sync.Mutex{},
		offsetFetcher: NewOffsetFetcher(store),
		latestOffsets: make(map[string]uint),
		synchronized:  make(map[string]bool),
	}
	d.cond = sync.NewCond(&d.mutex)

	return d
}

//Append adds message in the store
func (d *daakiya) Append(message AppendMessage) error {

	t := time.Now()
	if err := message.Validate(); err != nil {
		return err
	}
	if err := d.synchronize(message.Hash, message.Topic); err != nil {
		return err
	}

	d.cond.L.Lock()
	key := d.getOffsetKey(message.Hash, message.Topic)
	o := d.latestOffsets[key]
	var err error

	err = d.store.Put(storage.Message{
		Topic:  message.Topic,
		Hash:   message.Hash,
		Value:  message.Value,
		Offset: o,
	})
	if err == nil {
		o++
		d.latestOffsets[key] = o
	}

	d.cond.Broadcast()
	d.cond.L.Unlock()

	fmt.Println(fmt.Sprintf("append time: %d ms", time.Now().Sub(t).Milliseconds()))
	return err

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