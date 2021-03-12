package registry_test

// import (
// 	"context"
// 	"testing"
// 	"time"

// 	"github.com/minhajuddinkhan/daakiya/registry"
// 	"github.com/minhajuddinkhan/daakiya/storage"
// 	"github.com/stretchr/testify/assert"
// )

// func getRegistry() registry.Registry {
// 	return registry.NewRegistry(storage.NewKVStorage())
// }

// func TestRegistry_UnwrittenOffsetShouldReturnOffsetUnavailable(t *testing.T) {

// 	r := getRegistry()
// 	ctx := context.Background()
// 	channel, err := r.FromOffset(ctx, "12345", 0)
// 	assert.NotNil(t, err)
// 	assert.Nil(t, channel)
// 	assert.IsType(t, &storage.ErrHashNotFound{}, err)

// }

// func TestRegistry_ShouldFetchMessageThatIsWritten(t *testing.T) {

// 	r := getRegistry()

// 	hash := "11111111111"
// 	wMessage := []byte("test message")
// 	r.Append(hash, wMessage)

// 	ctx, cancelFunc := context.WithCancel(context.Background())
// 	defer cancelFunc()
// 	channel, err := r.FromOffset(ctx, hash, 0)
// 	assert.Nil(t, err)

// 	rMessage := <-channel
// 	assert.NotNil(t, rMessage)
// 	assert.Equal(t, rMessage, wMessage)

// }

// func TestRegistry_ShouldKeepListeningToMessagesIfNegativeOffset(t *testing.T) {
// 	r := getRegistry()

// 	hash := "11111111111"

// 	ctx, cancelFunc := context.WithCancel(context.Background())
// 	defer cancelFunc()

// 	channel, err := r.FromOffset(ctx, hash, -1)
// 	assert.Nil(t, err)

// 	timer := time.After(2 * time.Second)
// 	select {
// 	case <-timer:
// 		return

// 	case <-channel:
// 		t.Fail()
// 		return

// 	}
// }

// func TestRegistry_ShouldBeAbleToCloseChannelsIfWaitIsTooLong(t *testing.T) {

// 	r := getRegistry()

// 	hash := "11111111111"

// 	//deliberatelly defer cancelFunc avoided.
// 	ctx, cancelFunc := context.WithCancel(context.Background())

// 	channel, err := r.FromOffset(ctx, hash, -1)
// 	assert.Nil(t, err)

// 	timer := time.After(2 * time.Second)
// 	cancelFunc()

// 	passed := false
// 	go func() {
// 		<-channel
// 		passed = true
// 	}()
// 	<-timer

// 	assert.True(t, passed)

// }
