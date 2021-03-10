package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/minhajuddinkhan/mdelivery/daakiya"
	"github.com/minhajuddinkhan/mdelivery/registry"
	"github.com/minhajuddinkhan/mdelivery/storage"
)

func main() {

	// registry := registry.NewRegistry(storage.NewKVStorage())
	registries := map[string]registry.Registry{
		"12345": registry.NewRegistry(storage.NewKVStorage()),
		"11111": registry.NewRegistry(storage.NewKVStorage()),
	}
	d := daakiya.NewDaakiya(registries)

	go func() {
		i := 0
		j := 0
		for {
			for i < 2 {

				registries["12345"].Append([]byte(fmt.Sprintf("%d", j)))
				// registries["12345"].Append([]byte(fmt.Sprintf("%d", i)))

				// fmt.Println("appending...", j)
				i++
				j++
				// time.Sleep(time.Second)

			}
			time.Sleep(2 * time.Second)
			i = 0
		}
	}()

	http.HandleFunc("/recieve", d.EstablishWebsocketConnection())
	log.Fatal(http.ListenAndServe(":3000", nil))
}
