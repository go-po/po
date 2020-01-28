package po

import (
	"context"
	"github.com/go-po/po/internal/record"
	"sync"
)

type Handler interface {
	Handle(ctx context.Context, msg Message) error
}

type Appender interface {
	Append(message ...interface{}) error
}

type Stream struct {
	ID       string          // Unique ID of the stream
	ctx      context.Context // to use for the operation
	store    Store           // used to store records
	broker   Broker
	registry Registry

	mu      sync.Mutex      // guards below fields
	records []record.Record // All data
	size    int64           // number of records when the stream was first read
	read    bool            // have records been read from the store
}

func (stream *Stream) Append(messages ...interface{}) error {
	if len(messages) == 0 {
		return nil // nothing to do
	}

	err := stream.Load()
	if err != nil {
		return err
	}

	tx, err := stream.store.Begin(stream.ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	var records []record.Record
	for _, msg := range messages {
		b, err := stream.registry.Marshal(msg)
		if err != nil {
			return err
		}
		record := record.Record{
			Id:     stream.size + 1,
			Stream: stream.ID,
			Data:   b,
			Type:   stream.registry.LookupType(msg),
		}
		err = stream.store.Store(tx, record)
		if err != nil {
			return err
		}
		stream.size = stream.size + 1
		records = append(records, record)
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	stream.records = append(stream.records, records...)
	err = stream.broker.Notify(stream.ctx, records...)
	if err != nil {
		return err
	}
	return nil
}

func (stream *Stream) Project(projection interface{}) error {
	handler, isHandler := projection.(Handler)
	if isHandler {
		return stream.projectHandler(handler)
	}
	return nil
}

// can be used to define when data is loaded.
// usually happens during normal operations
func (stream *Stream) Load() error {
	stream.mu.Lock()
	defer stream.mu.Unlock()

	if stream.read {
		return nil
	}

	records, err := stream.store.ReadRecords(stream.ctx, stream.ID)
	if err != nil {
		return err
	}

	stream.records = records
	stream.read = true
	stream.size = int64(len(stream.records))
	return nil
}

func (stream *Stream) projectHandler(handler Handler) error {
	err := stream.Load()
	if err != nil {
		return err
	}
	for _, record := range stream.records {
		msg, err := ToMessage(stream.registry, record)
		if err != nil {
			return err
		}
		err = handler.Handle(stream.ctx, msg)
		if err != nil {
			return err
		}
	}
	return nil
}

// TODO this implementation needs to change as soon as snapshots is introduced
func (stream *Stream) Size() (int, error) {
	err := stream.Load()
	if err != nil {
		return 0, err
	}
	return len(stream.records), nil
}
