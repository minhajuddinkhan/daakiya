package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/ozonru/etcd/clientv3"
)

type etcd struct {
	cli       *clientv3.Client
	offsetKey string
}

func NewETCDStorage(endpoints []string) (Storage, error) {

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	return &etcd{cli: cli}, nil
}

func (e *etcd) getKey(hash, topic string, offset uint) string {
	return fmt.Sprintf("%s_%s_%d", hash, topic, offset)
}

func (e *etcd) getOffsetKey(hash, topic string) string {
	return fmt.Sprintf("%s_%s_%s", e.offsetKey, hash, topic)
}
func (e *etcd) Get(q Query) (*Message, error) {

	resp, err := e.cli.Get(context.Background(), e.getKey(q.Hash, q.Topic, uint(q.Offset)))
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, &OffsetNotFound{Message: "message not found"} //&OffsetNotFound{Message: "message not found"}
	}
	var msg map[string]interface{}

	if err := json.Unmarshal(resp.Kvs[0].Value, &msg); err != nil {
		return nil, err
	}

	return &Message{
		Topic:     q.Topic,
		Hash:      q.Hash,
		Value:     []byte(msg["data"].(string)),
		Offset:    uint(msg["offset"].(float64)),
		Timestamp: msg["timestamp"].(string),
	}, nil
}

func (e *etcd) Put(m Message) error {

	liese, err := e.cli.Grant(context.Background(), 5)
	if err != nil {
		return err
	}
	msg := map[string]interface{}{
		"data":      string(m.Value),
		"offset":    m.Offset,
		"timestamp": m.Timestamp,
	}

	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	resp, err := e.cli.Txn(context.Background()).
		Then(clientv3.OpPut(e.getKey(m.Hash, m.Topic, m.Offset),
			string(b),
			clientv3.WithLease(liese.ID),
		),
			clientv3.OpPut(
				e.getOffsetKey(m.Hash, m.Topic),
				strconv.Itoa(int(m.Offset)),
			),
		).Commit()

	if err != nil {
		return err
	}

	if resp.Succeeded {
		return nil
	}

	return fmt.Errorf("something went wrong")
}

func (e *etcd) Flush(hash string, topic string)                  {}
func (e *etcd) GetOldestOffset(hash, topic string) (uint, error) { return 0, nil }
func (e *etcd) GetLatestOffset(hash, topic string) (uint, error) {

	resp, err := e.cli.Get(context.Background(), e.getOffsetKey(hash, topic))
	if err != nil {
		return 0, err
	}

	if len(resp.Kvs) == 0 {
		return 0, &OffsetNotFound{Message: "offset not found in getting latest"}
	}

	var offset int

	offset, err = strconv.Atoi(string(resp.Kvs[0].Value))
	if err != nil {
		return 0, err
	}
	return uint(offset), nil
}
