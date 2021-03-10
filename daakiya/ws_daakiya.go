package daakiya

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
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

//EstablishWebsocketConnection establishes a web socket connection
func (d *Daakia) EstablishWebsocketConnection() http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {

		upgrader := websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		}

		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		// clientID := r.Header.Get("clientId")
		// if clientID == "" {
		// 	fmt.Println("invalid client id")
		// 	return
		// }

		clientID := "12345"

		offset, err := strconv.Atoi(r.URL.Query().Get("offset"))
		if err != nil {
			fmt.Println("ERROR?", err)
			http.Error(w, "invalid offset!", http.StatusInternalServerError)
			return
		}

		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Print("upgrade:", err)
			return
		}
		defer c.Close()

		ctx := context.Background()
		queue, cancelFunc := d.registries[clientID].FromOffset(ctx, uint(offset))
		defer cancelFunc()

		for {
			select {
			case <-r.Context().Done():
				fmt.Println("client disconnected")
				return
			case <-time.NewTicker(2 * time.Second).C:
				if err := c.WriteMessage(websocket.PingMessage, nil); err != nil {
					cancelFunc()
					return
				}
			case message := <-queue:
				if err := c.WriteMessage(1, message); err != nil {
					continue
				}

			}

		}
	}

}
