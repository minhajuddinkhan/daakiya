package registry_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/minhajuddinkhan/daakiya/registry"
	"github.com/minhajuddinkhan/daakiya/storage"
	"github.com/stretchr/testify/assert"
)

func TestRegistry_UnwrittenOffsetShouldReturnOffsetUnavailable(t *testing.T) {

	r := registry.NewRegistry(storage.NewKVStorage())
	ctx := context.Background()
	channel, err := r.FromOffset(ctx, "12345", 0)
	assert.NotNil(t, err)
	assert.Nil(t, channel)
	assert.IsType(t, &storage.ErrHashNotFound{}, err)

}

func TestRegistry_ShouldFetchMessageThatIsWritten(t *testing.T) {

	r := registry.NewRegistry(storage.NewKVStorage())

	hash := "11111111111"
	wMessage := []byte("test message")
	r.Append(hash, wMessage)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	channel, err := r.FromOffset(ctx, hash, 0)
	assert.Nil(t, err)

	rMessage := <-channel
	assert.NotNil(t, rMessage)
	assert.Equal(t, rMessage, wMessage)

}

func TestRegistry_ShouldKeepListeningToMessagesIfNegativeOffset(t *testing.T) {
	r := registry.NewRegistry(storage.NewKVStorage())

	hash := "11111111111"

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	fmt.Println("HERE??")
	channel, err := r.FromOffset(ctx, hash, -1)
	assert.Nil(t, err)

	fmt.Println("HERE?")
	timer := time.After(2 * time.Second)
	select {
	case <-timer:
		return

	case <-channel:
		t.Fail()
		return

	}

}
