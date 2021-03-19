package main

import (
	"fmt"
	"log"
	"time"

	daakiyaa "github.com/minhajuddinkhan/daakiya/daakiya"
	"github.com/minhajuddinkhan/daakiya/storage"
)

func main() {

	storage, err := storage.NewETCDStorage([]string{"localhost:2377", "localhost:2378", "localhost:2379"})
	if err != nil {
		log.Fatal(err)
	}

	dkw := daakiyaa.NewDaakiyaWriter(storage)

	start := time.Now()
	i := 0
	for {

		err = dkw.Write(daakiyaa.AppendMessage{
			Topic:     "X",
			Hash:      "1111",
			Value:     []byte("Hello"),
			Timestamp: time.Now(),
		})
		i++
		// time.Sleep(time.Second)
		if time.Now().Sub(start).Seconds() > 5 {
			fmt.Println("messages sent in 5 seconds", i)
			i = 0
			start = time.Now()
		}

	}
}
