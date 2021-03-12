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

	reg := registry.NewRegistry(storage)

	d := daakiya.NewDaakiya(reg)

	go func() {
		fmt.Println("waiting...")
		time.Sleep(5 * time.Second)
		fmt.Println("writing now..")
		i := 0
		j := 0
		for {
			for i < 5 {

				err := reg.Append(registry.Message{
					Topic: "LOCATION",
					Hash:  "12345",
					Value: []byte(fmt.Sprintf("%d", j)),
				})
				if err != nil {
					fmt.Println(err)
					log.Fatal(err)
				}

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
