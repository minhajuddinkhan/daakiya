package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gocql/gocql"
	daakiyaa "github.com/minhajuddinkhan/daakiya/daakiya"
	registry "github.com/minhajuddinkhan/daakiya/daakiya"
	"github.com/minhajuddinkhan/daakiya/storage"
)

func main() {

	reg := getRegistry()
	go func() {
		i := 0
		for {

			reg.Append(registry.AppendMessage{
				Topic: "TEST_TOPIC",
				Hash:  "CLIENT_1",
				Value: []byte(fmt.Sprintf("%d", i)),
			})
			time.Sleep(1 * time.Second)
			i++
		}
	}()

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	channel, err := reg.FromOffset(ctx, registry.Query{
		Hash:   "CLIENT_1",
		Topic:  "TEST_TOPIC",
		Offset: registry.OLDEST,
	})
	if err != nil {
		log.Fatal(err)
	}

	for {
		msg := <-channel
		fmt.Println(string(msg))
	}

}

func getRegistry() daakiyaa.Daakiya {

	clusterConfig := gocql.NewCluster("localhost:9042")
	clusterConfig.Keyspace = "test_keyspace"

	storage, err := storage.NewCassandraStorage(clusterConfig)
	if err != nil {
		log.Fatal(err)
	}

	return daakiyaa.NewDaakiya(storage)

}
