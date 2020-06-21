package po

import (
	"context"
	"errors"

	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/streams"
)

type executor interface {
	Execute(ctx context.Context, id streams.Id, lockPosition int64, cmd CommandHandler) (int64, error)
}

type executorFunc func(ctx context.Context, id streams.Id, lockPosition int64, cmd CommandHandler) (int64, error)

func (fn executorFunc) Execute(ctx context.Context, id streams.Id, lockPosition int64, cmd CommandHandler) (int64, error) {
	return fn(ctx, id, lockPosition, cmd)
}

func newExecutor(projector projector, appender appender) executorFunc {
	return func(ctx context.Context, id streams.Id, lockPosition int64, cmd CommandHandler) (int64, error) {
		position, err := projector.Project(ctx, id, lockPosition, cmd)
		if err != nil {
			return lockPosition, err
		}
		tx := &optimisticAppender{position: position}
		err = cmd.Execute(tx)
		if err != nil {
			return position, err
		}
		position, err = appender.Append(ctx, id, position, tx.messages...)
		if err != nil {
			return position, err
		}
		return position, nil
	}
}

type optimisticAppender struct {
	messages []interface{}
	position int64
}

func (appender *optimisticAppender) Append(messages ...interface{}) {
	appender.messages = append(appender.messages, messages...)
}

func (appender *optimisticAppender) Size() int64 {
	return appender.position
}

func newRetryExecutor(maxRetryCount int, inner executor) executorFunc {
	return func(ctx context.Context, id streams.Id, position int64, cmd CommandHandler) (int64, error) {
		retryCount := 0
		var err error
		for retryCount < maxRetryCount {
			position, err = inner.Execute(ctx, id, position, cmd)
			if errors.Is(err, store.WriteConflictError{}) {
				retryCount = retryCount + 1
				continue // attempt a retry
			}
			if err != nil {
				return position, err // unknown error, bail out
			}
			break // all went fine, exit the loop
		}
		return position, nil
	}
}
