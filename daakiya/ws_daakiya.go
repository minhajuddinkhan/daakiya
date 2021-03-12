package daakiya

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
	"github.com/minhajuddinkhan/daakiya/registry"
)

//Daakia Daakia
type Daakia struct {
	// store storage.Storage
	registry      registry.Registry
	Configuration Configuration
}

//NewDaakiya creates a new dakia instance
func NewDaakiya(registry registry.Registry) Daakia {
	return Daakia{
		registry:      registry,
		Configuration: NewConfig(),
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
			http.Error(w, "invalid offset!", http.StatusInternalServerError)
			return
		}

		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()

		query := registry.Query{Hash: clientID, Topic: "LOCATION", Offset: offset}
		queue, err := d.registry.FromOffset(ctx, query)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Print("upgrade:", err)
			return
		}
		defer c.Close()

		for {
			select {
			case <-r.Context().Done():
				fmt.Println("client disconnected")
				return
			case <-time.NewTicker(d.Configuration.ClientPingTime).C:
				if err := c.WriteMessage(websocket.PingMessage, nil); err != nil {
					cancelFunc()
					return
				}
			case message := <-queue:
				//channel is closed.
				if message == nil {
					return
				}
				if err := c.WriteMessage(1, message); err != nil {
					continue
				}

			}

		}
	}

}
