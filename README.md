


# Daakiya

## Description

Daakiya is a message broker that ensures 100% message delivery. It is inspired by Uber's ramen protocol built differntly on top of Go and cassandra.

![alt text](https://i.ibb.co/LdYQsZF/ramen.png)

![alt text](https://i.ibb.co/jzskrVP/daakiya.png)




## Example - Dakiya Writer

```go
func main() {

	storage, err := storage.NewETCDStorage([]string{"localhost:2377", "localhost:2378", "localhost:2379"})
	if err != nil {
		log.Fatal(err)
	}

	dkw := daakiyaa.NewDaakiyaWriter(storage)

	for {

		dkw.Write(daakiyaa.AppendMessage{
			Topic:     "X",
			Hash:      "1111",
			Value:     []byte("Hello"),
			Timestamp: time.Now(),
		})
		
	}
}


```

## Example - Dakiya Reader

```go


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

```