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
		ttl:           30,
		Mutex:         sync.Mutex{},
		//TODO:: take instead of arguments.
	}

	sess, err := c.clusterConfig.CreateSession()
	if err != nil {
		return nil, err
	}
	c.sess = sess

	err = c.sess.
		Query(`SELECT MAX(offset) FROM messages WHERE client_id = ? ALLOW FILTERING`, clientID).
		Scan(&c.offset)
	if err != nil {
		return nil, err
	}

	fmt.Println("setting offset at: ", c.offset)
	return c, nil
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
