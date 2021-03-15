package daakiyaa

import (
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"syscall"

	"github.com/gomodule/redigo/redis"
	"github.com/gorilla/websocket"
	"github.com/minhajuddinkhan/daakiya/storage"
)

type connection struct{}

type Courier interface {
	Deliver(message storage.Message) error
}

type redisCourier struct {
	redisPool   *redis.Pool
	connections map[string]*websocket.Conn
}

func NewCourier() Courier {

	pool := &redis.Pool{
		MaxIdle:   80,
		MaxActive: 12000,

		Dial: func() (redis.Conn, error) {
			fmt.Println("getting connection")
			conn, err := redis.Dial("tcp", "localhost:6379")
			if err != nil {
				log.Printf("ERROR: fail init redis pool: %s", err.Error())
				os.Exit(1)
			}
			return conn, err
		},
	}
	return &redisCourier{redisPool: pool, connections: make(map[string]*websocket.Conn)}
}

func (rc *redisCourier) Deliver(message storage.Message) error {

	conn := rc.redisPool.Get()
	defer conn.Close()

	recipient, err := redis.String(conn.Do("GET", message.Hash))
	if err != nil {
		fmt.Println("no client available...")
		return err
	}

	wconn, ok := rc.connections[message.Hash]
	if !ok {

		u := url.URL{Scheme: "ws", Host: recipient, Path: "/" + message.Topic}
		log.Printf("connecting to %s", u.String())

		c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		if err != nil {
			fmt.Println("no client available...")
			return err
		}
		rc.connections[message.Hash] = c
		wconn = c
	}

	if wconn.RemoteAddr().String() != recipient {

		u := url.URL{Scheme: "ws", Host: recipient, Path: "/" + message.Topic}
		log.Printf("connecting because remote does not match recipient to %s", u.String())

		c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		if err != nil {
			return err
		}
		rc.connections[message.Hash] = c
		wconn = c

	}
	err = wconn.WriteJSON(message)

	if errors.Is(err, syscall.EPIPE) {
		delete(rc.connections, message.Hash)
	}
	return err

}
