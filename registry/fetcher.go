package registry

import (
	"fmt"

	"github.com/minhajuddinkhan/daakiya/storage"
)

type Fetcher interface {
	Fetch(q Query) (uint, error)
}

type Query struct {
	Hash   string
	Topic  string
	Offset int
}

const LATEST = -1
const OLDEST = -2

type fetcher struct {
	store storage.Storage
}

func NewOffsetFetcher(store storage.Storage) Fetcher {
	return &fetcher{store: store}
}

func (f *fetcher) Fetch(q Query) (uint, error) {

	if q.Offset == LATEST {
		return f.store.GetLatestOffset(q.Hash, q.Topic)
	}

	if q.Offset == OLDEST {
		return f.store.GetOldestOffset(q.Hash, q.Topic)
	}
	if q.Offset < 0 {
		return 0, fmt.Errorf("invalid offset range provided. Must be -2, -1 or greater than 0")
	}

	currOldestOffset, currLatestOffset, err := f.getLimitingOffsets(q.Hash, q.Topic)
	if err != nil {
		return 0, err
	}

	if q.Offset > int(currLatestOffset) {
		return 0, &storage.OffsetNotFound{}
	}
	if q.Offset < int(currOldestOffset) {
		return 0, &storage.OffsetNotFound{}
	}

	return uint(q.Offset), nil
}

func (f *fetcher) getLimitingOffsets(hash, topic string) (oldest, latest uint, err error) {

	oldestOffset, err := f.store.GetOldestOffset(hash, topic)
	if err != nil {
		return 0, 0, err
	}

	latestOffset, err := f.store.GetLatestOffset(hash, topic)
	if err != nil {
		return 0, 0, err
	}

	return oldestOffset, latestOffset, nil
}
