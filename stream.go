package po

import (
	"context"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/stream"
	"sync"
)

type Appender interface {
	Append(message ...interface{}) error
}

type Stream struct {
	ID       stream.Id       // Unique ID of the stream
	ctx      context.Context // to use for the operation
	store    Store           // used to store records
	broker   Broker
	registry Registry

	mu      sync.Mutex      // guards below fields
	records []record.Record // All data
	size    int64           // number of records when the stream was first read
	read    bool            // have records been read from the store
}

func (s *Stream) Append(messages ...interface{}) error {
	if len(messages) == 0 {
		return nil // nothing to do
	}

	err := s.Load()
	if err != nil {
		return err
	}

	tx, err := s.store.Begin(s.ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	var records []record.Record
	for _, msg := range messages {
		b, contentType, err := s.registry.Marshal(msg)
		if err != nil {
			return err
		}
		record, err := s.store.StoreRecord(tx, s.ID, contentType, b)
		if err != nil {
			return err
		}
		s.size = s.size + 1
		records = append(records, record)
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	s.records = append(s.records, records...)
	err = s.broker.Notify(s.ctx, records...)
	if err != nil {
		return err
	}
	return nil
}

func (s *Stream) Project(projection interface{}) error {
	handler, isHandler := projection.(stream.Handler)
	if isHandler {
		return s.projectHandler(handler)
	}
	return nil
}

// can be used to define when data is loaded.
// usually happens during normal operations
func (s *Stream) Load() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.read {
		return nil
	}

	records, err := s.store.ReadRecords(s.ctx, s.ID)
	if err != nil {
		return err
	}

	s.records = records
	s.read = true
	s.size = int64(len(s.records))
	return nil
}

func (s *Stream) projectHandler(handler stream.Handler) error {
	err := s.Load()
	if err != nil {
		return err
	}
	for _, record := range s.records {
		msg, err := s.registry.ToMessage(record)
		if err != nil {
			return err
		}
		err = handler.Handle(s.ctx, msg)
		if err != nil {
			return err
		}
	}
	return nil
}

// TODO this implementation needs to change as soon as snapshots is introduced
func (s *Stream) Size() (int, error) {
	err := s.Load()
	if err != nil {
		return 0, err
	}
	return len(s.records), nil
}
