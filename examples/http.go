package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	daakiyaa "github.com/minhajuddinkhan/daakiya/daakiya"
	"github.com/minhajuddinkhan/daakiya/storage"
)

var addr = flag.String("addr", "localhost:3000", "http service address")

func handlePushRequest(writer daakiyaa.DaakiyaWriter) http.HandlerFunc {

	return func(rw http.ResponseWriter, r *http.Request) {
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}

		clientID := r.URL.Query().Get("client_id")
		if clientID == "" {
			fmt.Println("empty client id")
			rw.WriteHeader(http.StatusBadRequest)
			return
		}
		topic := r.URL.Query().Get("topic")
		if topic == "" {
			fmt.Println("empty topic")
			rw.WriteHeader(http.StatusBadRequest)
			return
		}

		writer.Write(daakiyaa.AppendMessage{
			Topic:     topic,
			Hash:      clientID,
			Value:     b,
			Timestamp: time.Now(),
		})
	}
}

func main() {

	//initializes daakiya backed by cassandra
	flag.Parse()
	log.SetFlags(0)

	storage, err := storage.NewETCDStorage([]string{"localhost:2377", "localhost:2378", "localhost:2379"})
	if err != nil {
		log.Fatal(err)
	}

	dkw := daakiyaa.NewDaakiyaWriter(storage)
	dkr := daakiyaa.NewDaakiyaReader(storage)
	http.HandleFunc("/push", handlePushRequest(dkw))

	client := "sample-client-id"
	topic := "sample-test-topic"

	go func() {
		for {
			for m := range dkr.Read(context.Background(), client, topic, 0) {
				fmt.Println("OUTPUT RETURNED:", m.Hash, string(m.Value), m.Topic, m.Offset)
			}
			time.Sleep(time.Second)
		}

	}()

	log.Fatal(http.ListenAndServe(*addr, nil))

}
