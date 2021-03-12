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
	offset   uint
	clientID string
	ttl      uint16
}

//NewCassandraStorage creates a new cassandra storage
func NewCassandraStorage(clientID string, clusterConfig *gocql.ClusterConfig) (Storage, error) {
	c := &cassandra{
		clientID:      clientID,
		clusterConfig: clusterConfig,
		ttl:           2,
		Mutex:         sync.Mutex{},
		//TODO:: take instead of arguments.
	}

	sess, err := c.clusterConfig.CreateSession()
	if err != nil {
		return nil, err
	}
	c.sess = sess

	c.offset, err = c.lastAvailableOffset()
	return c, err

}

func (c *cassandra) lastAvailableOffset() (uint, error) {

	sess, err := c.session()
	if err != nil {
		return 0, err
	}
	var lastAvailableOffset uint64
	err = sess.
		Query(`SELECT MAX(offset) FROM messages WHERE client_id = ? ALLOW FILTERING`, c.clientID).
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

func (c *cassandra) Append(message []byte) error {

	session, err := c.session()
	if err != nil {
		return err
	}

	q := `INSERT INTO messages
	(client_id, offset, data) VALUES (?, ?, ?)
	 USING TTL ?`

	args := []interface{}{
		c.clientID,
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
func (c *cassandra) Flush() {
	//do nothing for now...
}
func (c *cassandra) Get(offset uint64) ([]byte, error) {

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
	args := []interface{}{c.clientID, offset}

	var bytes []byte
	if err := session.Query(q, args...).Scan(&bytes); err != nil {
		if err == gocql.ErrNotFound {
			return nil, &OffsetNotFound{message: err.Error()}
		}
		return nil, err
	}
	return bytes, nil
}

func (c *cassandra) GetLastAvailableOffset() (uint, error) {
	return c.lastAvailableOffset()
}
