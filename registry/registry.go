package registry

import (
	"context"
	"fmt"
	"sync"

	"github.com/minhajuddinkhan/mdelivery/storage"
)

//Registry Registry
type Registry interface {
	Append(message []byte)
	FromOffset(context context.Context, offset uint) (chan []byte, context.CancelFunc)
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
func (r *registry) Append(message []byte) {
	r.cond.L.Lock()
	r.cond.Broadcast()
	r.store.Append(message)
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
func (r *registry) FromOffset(ctx context.Context, offset uint) (chan []byte, context.CancelFunc) {

	wCancel, cancelFunc := context.WithCancel(ctx)
	ch := make(chan []byte)
	go func() {
		for {

			select {
			case <-wCancel.Done():
				fmt.Println("closing channell..")
				close(ch)
				return
			default:
				val, err := r.store.Get(uint64(offset))
				if err != nil {
					fmt.Println("ERROR?", err)
					switch err.(type) {
					case *storage.OffsetNotFound:
						<-r.nextMessageAvailable()
						continue

					default:
						close(ch)
						fmt.Println("closing chanell in defaults ection..")
						return
					}
				}
				ch <- val
				offset++
			}

		}
	}()
	return ch, cancelFunc

}
