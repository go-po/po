package po

import (
	"context"

	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/streams"
)

type appender interface {
	Append(ctx context.Context, id streams.Id, position int64, messages ...interface{}) (int64, error)
}

type appenderFunc func(ctx context.Context, id streams.Id, position int64, messages ...interface{}) (int64, error)

func (fn appenderFunc) Append(ctx context.Context, id streams.Id, position int64, messages ...interface{}) (int64, error) {
	return fn(ctx, id, position, messages...)
}

type appenderStore interface {
	WriteRecords(ctx context.Context, id streams.Id, data ...record.Data) (int64, error)
	WriteRecordsFrom(ctx context.Context, id streams.Id, position int64, data ...record.Data) error
}

func newAppenderFunc(store appenderStore, registry Registry) appenderFunc {
	return func(ctx context.Context, id streams.Id, position int64, messages ...interface{}) (int64, error) {
		var data []record.Data
		for _, msg := range messages {
			b, contentType, err := registry.Marshal(msg)
			if err != nil {
				return -1, err
			}
			data = append(data, record.Data{
				ContentType: contentType,
				Data:        b,
			})
		}

		if position < 0 {
			// this append have not seen the lockPosition yet,
			// so have to get it from the store when performing the first write
			return store.WriteRecords(ctx, id, data...)
		}

		err := store.WriteRecordsFrom(ctx, id, position, data...)
		if err != nil {
			return -1, err
		}

		return position + int64(len(messages)), nil
	}
}
