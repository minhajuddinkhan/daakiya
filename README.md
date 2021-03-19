


# Daakiya

## Description

Daakiya is a message broker that ensures 100% message delivery. It is inspired by Uber's ramen protocol built differntly on top of Go with etcd.

![alt text](https://i.ibb.co/LdYQsZF/ramen.png)

![alt text](https://i.ibb.co/ws8xFJg/etcd.png)




## Example - Dakiya Writer

```go
func main() {

	storage, err := storage.NewETCDStorage([]string{
		"localhost:2377",
		"localhost:2378",
		"localhost:2379",
	}))

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

	storage, err := storage.NewETCDStorage([]string{
		"localhost:2377",
		"localhost:2378",
		"localhost:2379",
	})

	if err != nil {
		log.Fatal(err)
	}

	dkr := daakiyaa.NewDaakiyaReader(storage)
	for m := range dkr.Read(context.Background(), "1111", "X", 0) {
		fmt.Println("OUTPUT RETURNED:", m.Hash, string(m.Value), m.Topic)
	}
}

```