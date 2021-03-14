


# Daakiya

## Description

Daakiya is a message broker that ensures 100% message delivery. It is inspired by Uber's ramen protocol built differntly on top of Go and cassandra.

![alt text](https://filebin.net/ahqhthw95we9bnex/ramen.png?t=75zdd8lj)

![alt text](https://i.ibb.co/jzskrVP/daakiya.png)




## Example

```go
func main() {

	//initializes daakiya backed by cassandra
	dk := getDaakiya()

	go func() {
		//start an thread that pushes messages
		//to daakiya every second
		i := 0
		for {
			dk.Append(registry.AppendMessage{
				Topic: "TEST_TOPIC",
				Hash:  "CLIENT_1",
				Value: []byte(fmt.Sprintf("Hello my friend %d", i)), //Hello my friend {i}
			})
			time.Sleep(1 * time.Second)
			i++
		}
	}()

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	//start reading messages from the provided offset
	channel, err := dk.FromOffset(ctx, registry.Query{
		Hash:   "CLIENT_1",
		Topic:  "TEST_TOPIC",
		Offset: registry.OLDEST,
	})

	if err != nil {
		log.Fatal(err)
	}

	for {
		msg := <-channel
		fmt.Println(string(msg)) //Hello my friend {i}
	}

}
```
