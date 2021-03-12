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

func (c *cassandra) lastAvailableOffset(hash string) (uint, error) {

	sess, err := c.session()
	if err != nil {
		return 0, err
	}
	var lastAvailableOffset uint64
	err = sess.
		Query(`SELECT MIN(offset) FROM messages WHERE client_id = ? ALLOW FILTERING`, hash).
		Scan(&lastAvailableOffset)
	if err != nil {
		return 0, err
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

func (c *cassandra) synchronizeOffset(hash string) error {

	c.Lock()
	if !c.synchronized {
		o, err := c.lastAvailableOffset(hash)
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
func (c *cassandra) Append(hash string, message []byte) error {

	session, err := c.session()
	if err != nil {
		return err
	}

	if err := c.synchronizeOffset(hash); err != nil {
		return err
	}

	q := `INSERT INTO messages
	(client_id, offset, data) VALUES (?, ?, ?)
	 USING TTL ?`

	args := []interface{}{
		hash,
		c.offset,
		message,
		c.ttl,
	}
	if err = session.Query(q, args...).Exec(); err != nil {
		if err == gocql.ErrNotFound {
			return &OffsetNotFound{message: err.Error()}
		}
		return err
	}
	c.offset++
	return nil
}
func (c *cassandra) Flush(hash string) {
	//do nothing for now...
}
func (c *cassandra) Get(hash string, offset uint64) ([]byte, error) {

	c.Lock()
	if offset > uint64(c.offset) {
		c.Unlock()
		return nil, &OffsetUnavailable{Message: fmt.Sprintf("message not written on offset %d yet", offset)}
	}
	c.Unlock()

	session, err := c.session()
	if err != nil {
		return nil, err
	}

	q := `SELECT data FROM messages WHERE client_id = ? AND offset = ?`
	args := []interface{}{hash, offset}

	var bytes []byte
	if err := session.Query(q, args...).Scan(&bytes); err != nil {
		if err == gocql.ErrNotFound {
			return nil, &OffsetNotFound{message: err.Error()}
		}
		return nil, err
	}
	return bytes, nil
}

func (c *cassandra) GetLastAvailableOffset(hash string) (uint, error) {
	return c.lastAvailableOffset(hash)
}
