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

func (e *etcd) getKey(hash, topic string) string {
	return fmt.Sprintf("%s_%s", hash, topic)
}

func (e *etcd) getOffsetKey(hash, topic string) string {
	return fmt.Sprintf("%s_%s_%s", e.offsetKey, hash, topic)
}

func (e *etcd) ReadFrom(ctx context.Context, q Query) chan Message {

	wCtx, cancelFunc := context.WithCancel(ctx)

	ch := make(chan Message)

	k := e.getKey(q.Hash, q.Topic)

	watchChannel := e.cli.Watch(wCtx, k, clientv3.WithRev(int64(q.Offset)))
	go func(wc clientv3.WatchChan) {
		defer func() {
			cancelFunc()
			close(ch)
		}()

		select {
		case <-wCtx.Done():
			return
		default:

			for watchResponse := range watchChannel {

				if err := watchResponse.Err(); err != nil {
					return
				}
				if watchResponse.Canceled {
					return
				}

				for _, event := range watchResponse.Events {
					select {
					case <-wCtx.Done():
						return
					default:
						ch <- Message{
							Topic:  q.Topic,
							Value:  []byte(event.Kv.Value),
							Offset: uint(event.Kv.CreateRevision),
							Hash:   q.Hash,
						}
					}
				}
			}

		}
	}(watchChannel)

	return ch
}

func (e *etcd) Put(m Message) error {

	liese, err := e.cli.Grant(context.Background(), 30)
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

	put := clientv3.OpPut(e.getKey(m.Hash, m.Topic), string(b), clientv3.WithLease(liese.ID))
	resp, err := e.cli.
		Txn(context.Background()).
		Then(put).
		Commit()

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
