package registry

import (
	"fmt"
	"sync"

	"github.com/minhajuddinkhan/mdelivery/storage"
)

//Registry Registry
type Registry interface {
	Append(message []byte)
	FromOffset(offset uint) chan []byte
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

	fmt.Println("wait state....")
	r.cond.Wait()
	go func(channel chan struct{}) {

		channel <- struct{}{}
	}(c)
	r.cond.L.Unlock()
	return c
}

//FromOffset returns a channel that provides all available messages
func (r *registry) FromOffset(offset uint) chan []byte {

	ch := make(chan []byte)
	go func() {
		for {
			val, err := r.store.Get(uint64(offset))
			if err != nil {
				switch err.(type) {
				case *storage.OffsetNotFound:
					<-r.nextMessageAvailable()

				default:
					close(ch)
					return

				}
			}
			ch <- val
			offset++
		}
	}()
	return ch

}
