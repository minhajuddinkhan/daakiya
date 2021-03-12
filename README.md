


# Daakiya

## Description

Daakiya is a message broker that ensures 100% message delivery. It is inspired by Uber's ramen protocol built differntly on top of Go and cassandra.

![alt text](https://filebin.net/ahqhthw95we9bnex/ramen.png?t=4cvahb3w
)





## Example

```go
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
```
