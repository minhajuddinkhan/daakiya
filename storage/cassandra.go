package storage

import (
	"github.com/gocql/gocql"
)

type cassandra struct {
	clusterConfig *gocql.ClusterConfig
	sess          *gocql.Session
	ttl           uint16
}

//NewCassandraStorage creates a new cassandra storage
func NewCassandraStorage(clusterConfig *gocql.ClusterConfig) (Storage, error) {

	c := &cassandra{
		clusterConfig: clusterConfig,
		ttl:           30,
		//TODO:: take instead of arguments.
	}

	sess, err := c.clusterConfig.CreateSession()
	if err != nil {
		return nil, err
	}
	c.sess = sess

	return c, err
}

func (c *cassandra) Put(m Message) error {

	session, err := c.session()
	if err != nil {
		return err
	}

	q := `INSERT INTO messages
	(hash, topic, offset, data) VALUES (?, ?, ?, ?)
	 USING TTL ?`

	args := []interface{}{
		m.Hash,
		m.Topic,
		m.Offset,
		m.Value,
		c.ttl,
	}

	if err = session.Query(q, args...).Exec(); err != nil {
		if err == gocql.ErrNotFound {
			return &OffsetNotFound{Message: err.Error()}
		}
		return err
	}

	return nil
}

func (c *cassandra) Get(q Query) ([]byte, error) {

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

func (c *cassandra) GetLatestOffset(hash, topic string) (uint, error) {
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
