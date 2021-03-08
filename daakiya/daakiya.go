package daakiya

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/minhajuddinkhan/mdelivery/registry"
)

//Daakia Daakia
type Daakia struct {
	// store storage.Storage
	registries map[string]registry.Registry
}

//NewDaakiya creates a new dakia instance
func NewDaakiya(registry map[string]registry.Registry) Daakia {
	return Daakia{
		registries: registry,
	}
}

func (d *Daakia) ServeHTTP(rw http.ResponseWriter, r *http.Request) {

	flusher, ok := rw.(http.Flusher)
	if !ok {
		http.Error(rw, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}

	offset, err := strconv.Atoi(r.URL.Query().Get("offset"))
	if err != nil {
		http.Error(rw, "invalid offset!", http.StatusInternalServerError)
		return

	}
	clientID := r.Header.Get("clientID")
	clientID = "12345"
	if _, ok := d.registries[clientID]; !ok {
		http.Error(rw, "invalid client id", http.StatusBadRequest)
		return
	}

	rw.Header().Set("Content-Type", "text/event-stream")
	rw.Header().Set("Cache-Control", "no-cache")
	rw.Header().Set("Connection", "keep-alive")
	rw.Header().Set("Access-Control-Allow-Origin", "*")

	for {
		ctx := context.Background()
		queue, cancelFunc := d.registries[clientID].FromOffset(ctx, uint(offset))
		defer cancelFunc()

		select {
		case <-r.Context().Done():
			fmt.Println("client disconnected")
			return

		case message := <-queue:
			fmt.Fprintf(rw, "data: %v\n", string(message))
			flusher.Flush()
			offset++

		}

	}

}
