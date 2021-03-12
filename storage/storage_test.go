package storage_test

// import (
// 	"testing"

// 	"github.com/minhajuddinkhan/daakiya/storage"
// 	"github.com/stretchr/testify/assert"
// )

// func TestStorage_ShouldAppendAndReturnCorrectValueOnOffset(t *testing.T) {

// 	store := storage.NewKVStorage()
// 	store.Append("12345", []byte("hello"))
// 	v, err := store.Get("12345", 0)
// 	assert.Nil(t, err)
// 	assert.Equal(t, "hello", string(v))
// }

// func TestStorage_ShouldReturnCorrectValueAfterFlush(t *testing.T) {

// 	store := storage.NewKVStorage()
// 	store.Append("12345", []byte("hello"))
// 	store.Append("12345", []byte("hey!"))

// 	store.Flush("12345")
// 	store.Append("12345", []byte("bye"))

// 	v, err := store.Get("12345", 0)
// 	assert.Nil(t, err)
// 	assert.Equal(t, []byte("bye"), v)
// }
