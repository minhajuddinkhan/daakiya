package main

import (
	"context"
	"encoding/json"
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

	dkr := daakiyaa.NewDaakiyaReader(storage)

	for {

		for m := range dkr.Read(context.Background(), "1111", "X", 0) {
			data := map[string]interface{}{}
			json.Unmarshal(m.Value, &data)

			t, _ := time.Parse(time.RFC1123, data["timestamp"].(string))
			fmt.Println(time.Now().Sub(t).Seconds())
			fmt.Println("OUTPUT RETURNED:", m.Hash, data["timestamp"], string(m.Value), m.Topic, m.Offset)
		}

		time.Sleep(time.Second)
		fmt.Println("retrying..")

	}

}
