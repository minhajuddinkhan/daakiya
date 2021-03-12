package registry

import (
	"github.com/minhajuddinkhan/daakiya/storage"
)

type Fetcher interface {
	Fetch(hash string, offset int) (uint, error)
}

const LATEST = -1
const OLDEST = -2

type fetcher struct {
	store storage.Storage
}

func NewOffsetFetcher(store storage.Storage) Fetcher {
	return &fetcher{store: store}
}

func (f *fetcher) Fetch(hash string, offset int) (uint, error) {

	if offset == LATEST {
		return f.store.GetLatestOffset(hash)
	}

	if offset == OLDEST {
		return f.store.GetOldestOffset(hash)
	}

	currOldestOffset, currLatestOffset, err := f.getLimitingOffsets(hash)
	if err != nil {
		return 0, err
	}

	if offset > int(currLatestOffset) {
		return 0, &storage.OffsetNotFound{}
	}
	if offset < int(currOldestOffset) {
		return 0, &storage.OffsetNotFound{}
	}

	return uint(offset), nil
}

func (f *fetcher) getLimitingOffsets(hash string) (oldest, latest uint, err error) {

	oldestOffset, err := f.store.GetOldestOffset(hash)
	if err != nil {
		return 0, 0, err
	}

	latestOffset, err := f.store.GetLatestOffset(hash)
	if err != nil {
		return 0, 0, err
	}

	return oldestOffset, latestOffset, nil
}
