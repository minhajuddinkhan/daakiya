package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gocql/gocql"
	"github.com/minhajuddinkhan/mdelivery/daakiya"
	"github.com/minhajuddinkhan/mdelivery/registry"
	"github.com/minhajuddinkhan/mdelivery/storage"
)

func main() {

	// registry := registry.NewRegistry(storage.NewKVStorage())

	clusterConfig := gocql.NewCluster("localhost:9042")
	clusterConfig.Keyspace = "test_keyspace"

	cassandraStorage12345, err := storage.NewCassandraStorage("12345", clusterConfig)
	if err != nil {
		log.Fatal(err)
	}

	registries := map[string]registry.Registry{
		"12345": registry.NewRegistry(cassandraStorage12345),
	}

	d := daakiya.NewDaakiya(registries)

	go func() {
		i := 0
		j := 0
		for {
			for i < 5 {

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
