package storage_test

import (
	"testing"

	"github.com/minhajuddinkhan/daakiya/storage"
	"github.com/stretchr/testify/assert"
)

func TestStorage_ShouldAppendAndReturnCorrectValueOnOffset(t *testing.T) {

	store := storage.NewKVStorage()
	store.Append([]byte("hello"))
	v, err := store.Get(0)
	assert.Nil(t, err)
	assert.Equal(t, "hello", string(v))
}

func TestStorage_ShouldReturnCorrectValueAfterFlush(t *testing.T) {

	store := storage.NewKVStorage()
	store.Append([]byte("hello"))
	store.Append([]byte("hey!"))

	store.Flush()
	store.Append([]byte("bye"))

	v, err := store.Get(2)
	assert.Nil(t, err)
	assert.Equal(t, []byte("bye"), v)
}
