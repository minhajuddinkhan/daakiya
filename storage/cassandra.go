package storage

import (
	"fmt"
	"sync"

	"github.com/gocql/gocql"
)

type cassandra struct {
	clusterConfig *gocql.ClusterConfig
	sess          *gocql.Session
	sync.Mutex
	offset       uint
	ttl          uint16
	synchronized bool
}

//NewCassandraStorage creates a new cassandra storage
func NewCassandraStorage(clusterConfig *gocql.ClusterConfig) (Storage, error) {

	c := &cassandra{
		clusterConfig: clusterConfig,
		ttl:           30,
		Mutex:         sync.Mutex{},
		//TODO:: take instead of arguments.
	}

	sess, err := c.clusterConfig.CreateSession()
	if err != nil {
		return nil, err
	}
	c.sess = sess

	return c, err

}

func (c *cassandra) Append(m Message) error {

	session, err := c.session()
	if err != nil {
		return err
	}

	if err := c.synchronizeOffsetOnStart(m.Hash, m.Topic); err != nil {
		return err
	}

	q := `INSERT INTO messages
	(hash, topic, offset, data) VALUES (?, ?, ?, ?)
	 USING TTL ?`

	args := []interface{}{
		m.Hash,
		m.Topic,
		c.offset,
		m.Value,
		c.ttl,
	}

	c.Lock()
	defer c.Unlock()
	if err = session.Query(q, args...).Exec(); err != nil {
		if err == gocql.ErrNotFound {
			return &OffsetNotFound{Message: err.Error()}
		}
		return err
	}
	c.offset++
	return nil
}

func (c *cassandra) Get(q Query) ([]byte, error) {

	c.Lock()

	if q.Offset > uint64(c.offset) {
		c.Unlock()
		return nil, &OffsetNotFound{Message: fmt.Sprintf("message not written on offset %d yet", q.Offset)}
	}
	c.Unlock()

	session, err := c.session()
	if err != nil {
		return nil, err
	}

	query := `SELECT data FROM messages WHERE hash = ? AND offset = ? AND topic = ?`
	args := []interface{}{q.Hash, q.Offset, q.Topic}

	var bytes []byte
	if err := session.Query(query, args...).Scan(&bytes); err != nil {

		if err == gocql.ErrNotFound {
			return nil, &OffsetNotFound{Message: err.Error()}
		}
		return nil, err
	}
	return bytes, nil
}

func (c *cassandra) Flush(hash, topic string) {
	//do nothing for now...
}

func (c *cassandra) GetOldestOffset(hash, topic string) (uint, error) {
	return c.oldestOffset(hash, topic)
}

func (c *cassandra) GetLatestOffset(hash, topic string) (uint, error) {
	return c.latestOffset(hash, topic)
}

func (c *cassandra) oldestOffset(hash, topic string) (uint, error) {

	sess, err := c.session()
	if err != nil {
		return 0, err
	}
	var lastAvailableOffset uint64
	err = sess.
		Query(`SELECT MIN(offset) FROM messages WHERE hash = ? AND topic = ? ALLOW FILTERING`, hash, topic).
		Scan(&lastAvailableOffset)
	if err != nil {
		return 0, &ErrHashNotFound{Message: err.Error()}
	}

	return uint(lastAvailableOffset), nil

}

func (c *cassandra) latestOffset(hash, topic string) (uint, error) {

	sess, err := c.session()
	if err != nil {
		return 0, err
	}
	var lastAvailableOffset uint64
	err = sess.
		Query(`SELECT MAX(offset) FROM messages WHERE hash = ? AND topic = ? ALLOW FILTERING`, hash, topic).
		Scan(&lastAvailableOffset)
	if err != nil {
		return 0, &ErrHashNotFound{Message: err.Error()}
	}

	return uint(lastAvailableOffset), nil

}

func (c *cassandra) session() (*gocql.Session, error) {
	if c.sess.Closed() {
		session, err := c.clusterConfig.CreateSession()
		if err != nil {
			return nil, err
		}
		c.sess = session
	}
	return c.sess, nil
}

func (c *cassandra) synchronizeOffsetOnStart(hash string, topic string) error {

	c.Lock()
	if !c.synchronized {
		o, err := c.oldestOffset(hash, topic)
		if err != nil {
			c.Unlock()
			return err
		}
		c.offset = o
		c.synchronized = true
	}
	c.Unlock()
	return nil

}
