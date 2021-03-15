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

	// time.Sleep(4 * time.Second)
	//start an thread that pushes messages
	//to daakiya every second

	go func() {
		for {

			ctx, cancelFunc := context.WithCancel(context.Background())
			defer cancelFunc()

			time.Sleep(1 * time.Second)
			//start reading messages from the provided offset
			ch, err := dk.FromOffset(ctx, registry.Query{
				Hash:   "CLIENT_1",
				Topic:  "TEST_TOPIC",
				Offset: daakiyaa.OLDEST,
			})
			if err != nil {
				log.Fatal(err)
			}
			<-ch
			time.Sleep(1 * time.Second)

		}
	}()

	i := 0
	j := 0

	tt := time.Now()
	for j = 0; j < 20; j++ {
		i = 0
		t := time.Now()
		for i < 1000 {
			err := dk.Append(registry.AppendMessage{
				Topic:     "TEST_TOPIC",
				Hash:      "CLIENT_1",
				Value:     []byte(fmt.Sprintf("Hello my friend %d", i)), //Hello my friend {i}
				Timestamp: time.Now(),
			})
			if err != nil {
				spew.Dump(err)
				fmt.Println(err)
			}

			i++
			// fmt.Println("Message written to Cassandra")
		}
		fmt.Println(fmt.Sprintf("wrote %d messages in %f seconds", i, time.Now().Sub(t).Seconds()))
		time.Sleep(1000 * time.Millisecond)

		// time.Sleep(10 * time.Millisecond)
	}

	fmt.Println(fmt.Sprintf("done writing %d  messages. finish-time: %v, total time to write: %f", i*j, time.Now(), time.Now().Sub(tt).Seconds()))

	// if err != nil {
	// 	spew.Dump(err)
	// 	log.Fatal(err)
	// }

	// for {
	// 	msg := <-channel
	// 	fmt.Println(string(msg)) //Hello my friend {i}
	// }

	time.Sleep(10 * time.Second)

}

func getDaakiya() daakiyaa.Daakiya {

	clusterConfig := gocql.NewCluster("localhost:9042")
	clusterConfig.Keyspace = "daakiya"

	storage, err := storage.NewCassandraStorage(clusterConfig)
	if err != nil {
		log.Fatal(err)
	}

	c := daakiyaa.NewCourier()
	return daakiyaa.NewDaakiya(storage, c)

}
