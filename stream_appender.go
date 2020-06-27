package po

import (
	"context"
	"encoding/json"

	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/registry"
	"github.com/go-po/po/streams"
)

type Msg struct {
	Name string
}

var testRegistry = registry.New()

func init() {
	testRegistry.Register(func(b []byte) (interface{}, error) {
		msg := Msg{}
		err := json.Unmarshal(b, &msg)
		return msg, err
	})
}

type appender interface {
	Append(ctx context.Context, id streams.Id, position int64, messages ...interface{}) (int64, error)
}

type appenderFunc func(ctx context.Context, id streams.Id, position int64, messages ...interface{}) (int64, error)

func (fn appenderFunc) Append(ctx context.Context, id streams.Id, position int64, messages ...interface{}) (int64, error) {
	return fn(ctx, id, position, messages...)
}

type appenderStore interface {
	WriteRecords(ctx context.Context, id streams.Id, data ...record.Data) ([]record.Record, error)
	WriteRecordsFrom(ctx context.Context, id streams.Id, position int64, data ...record.Data) ([]record.Record, error)
}

type notifier interface {
	Notify(ctx context.Context, positions ...record.Record) error
}

func newAppenderFunc(store appenderStore, notify notifier, registry Registry) appenderFunc {
	return func(ctx context.Context, id streams.Id, position int64, messages ...interface{}) (int64, error) {
		if len(messages) == 0 {
			return position, nil
		}
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

		var written []record.Record
		var err error
		if position < 0 {
			// this append have not seen the lockPosition yet,
			// so have to get it from the store when performing the first write
			written, err = store.WriteRecords(ctx, id, data...)
		} else {
			written, err = store.WriteRecordsFrom(ctx, id, position, data...)
		}

		if err != nil {
			return -1, err
		}

		for _, r := range written {
			// find max
			if r.Number > position {
				position = r.Number
			}
		}
		err = notify.Notify(ctx, written...)
		return position, err
	}
}
