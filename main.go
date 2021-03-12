package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gocql/gocql"
	"github.com/minhajuddinkhan/daakiya/daakiya"
	"github.com/minhajuddinkhan/daakiya/registry"
	"github.com/minhajuddinkhan/daakiya/storage"
)

func main() {

	// registry := registry.NewRegistry(storage.NewKVStorage())

	clusterConfig := gocql.NewCluster("localhost:9042")
	clusterConfig.Keyspace = "test_keyspace"

	storage, err := storage.NewCassandraStorage(clusterConfig)
	if err != nil {
		log.Fatal(err)
	}

	registry := registry.NewRegistry(storage)

	d := daakiya.NewDaakiya(registry)

	go func() {
		i := 0
		j := 0
		for {
			for i < 5 {

				registry.Append("12345", []byte(fmt.Sprintf("%d", j)))
				// registries["12345"].Append([]byte(fmt.Sprintf("%d", i)))

				// fmt.Println("appending...", j)
				i++
				j++
				// time.Sleep(time.Second)

			}
			time.Sleep(200 * time.Millisecond)
			i = 0
		}
	}()

	http.HandleFunc("/recieve", d.EstablishWebsocketConnection())
	log.Fatal(http.ListenAndServe(":3000", nil))
}
