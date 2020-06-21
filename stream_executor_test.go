package po

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-po/po/streams"
	"github.com/stretchr/testify/assert"
)

func newStubCmd(messageCount int) *stubCmd {
	var messages []interface{}
	for i := 0; i < messageCount; i++ {
		messages = append(messages, fmt.Sprintf("message %d", i))
	}
	return &stubCmd{
		messages: messages,
	}
}

type stubCmd struct {
	messages []interface{}
}

func (cmd *stubCmd) Handle(ctx context.Context, msg streams.Message) error {
	return nil
}

func (cmd *stubCmd) Execute(appender TransactionAppender) error {
	if len(cmd.messages) > 0 {
		appender.Append(cmd.messages...)
	}
	return nil
}

func TestExecutorFunc_Execute(t *testing.T) {
	ctx := context.Background()
	streamId := streams.ParseId("executor-1")
	t.Run("empty stream", func(t *testing.T) {
		// setup
		p := projectorFunc(func(ctx context.Context, id streams.Id, lockPosition int64, projection Handler) (int64, error) {
			return 0, nil
		})
		a := appenderFunc(func(ctx context.Context, id streams.Id, position int64, messages ...interface{}) (int64, error) {
			return position, nil
		})
		e := newExecutor(p, a)
		// execute
		pos, err := e.Execute(ctx, streamId, -1, newStubCmd(0))
		// verify
		assert.NoError(t, err)
		assert.Equal(t, 0, int(pos))
	})

	t.Run("stream in progress", func(t *testing.T) {
		// setup
		p := projectorFunc(func(ctx context.Context, id streams.Id, lockPosition int64, projection Handler) (int64, error) {
			return 10, nil
		})
		a := appenderFunc(func(ctx context.Context, id streams.Id, position int64, messages ...interface{}) (int64, error) {
			assert.Equal(t, 10, int(position), "append start position")
			return position, nil
		})
		e := newExecutor(p, a)
		// execute
		pos, err := e.Execute(ctx, streamId, -1, newStubCmd(0))
		// verify
		assert.NoError(t, err)
		assert.Equal(t, 10, int(pos))
	})

	t.Run("cmd with data", func(t *testing.T) {
		// setup
		p := projectorFunc(func(ctx context.Context, id streams.Id, lockPosition int64, projection Handler) (int64, error) {
			return -1, nil
		})
		a := appenderFunc(func(ctx context.Context, id streams.Id, position int64, messages ...interface{}) (int64, error) {
			return int64(len(messages)), nil
		})
		e := newExecutor(p, a)
		// execute
		pos, err := e.Execute(ctx, streamId, -1, newStubCmd(15))
		// verify
		assert.NoError(t, err)
		assert.Equal(t, 15, int(pos))
	})
}
