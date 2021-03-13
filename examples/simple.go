package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/gocql/gocql"
	daakiyaa "github.com/minhajuddinkhan/daakiya/daakiya"
	registry "github.com/minhajuddinkhan/daakiya/daakiya"
	"github.com/minhajuddinkhan/daakiya/storage"
)

func main() {

	//initializes daakiya backed by cassandra
	dk := getDaakiya()

	go func() {
		time.Sleep(4 * time.Second)
		// fmt.Println("writing...")
		//start an thread that pushes messages
		//to daakiya every second
		i := 0
		for {
			dk.Append(registry.AppendMessage{
				Topic: "TEST_TOPIC",
				Hash:  "CLIENT_1",
				Value: []byte(fmt.Sprintf("Hello my friend %d", i)), //Hello my friend {i}
			})
			dk.Append(registry.AppendMessage{
				Topic: "TEST_TOPIC_2",
				Hash:  "CLIENT_1",
				Value: []byte(fmt.Sprintf("Hello my friend %d", i)), //Hello my friend {i}
			})

			time.Sleep(1 * time.Second)
			i++
		}
	}()

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	time.Sleep(1 * time.Second)
	//start reading messages from the provided offset
	channel, err := dk.FromOffset(ctx, registry.Query{
		Hash:   "CLIENT_1",
		Topic:  "TEST_TOPIC",
		Offset: daakiyaa.LATEST,
	})

	if err != nil {
		spew.Dump(err)
		log.Fatal(err)
	}

	for {
		msg := <-channel
		fmt.Println(string(msg)) //Hello my friend {i}
	}

}

func getDaakiya() daakiyaa.Daakiya {

	clusterConfig := gocql.NewCluster("localhost:9042")
	clusterConfig.Keyspace = "test_keyspace"

	storage, err := storage.NewCassandraStorage(clusterConfig)
	if err != nil {
		log.Fatal(err)
	}

	return daakiyaa.NewDaakiya(storage)

}
