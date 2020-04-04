package po

import (
	"context"
	"encoding/json"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/stream"
	"sync"
)

type CommitAppender interface {
	AppendCommit(messages ...stream.Message) error
}
type MessageAppender interface {
	Append(messages ...interface{})
	Size() (int64, error)
}

type Executor interface {
	Execute(appender MessageAppender) error
}

type Stream struct {
	logger Logger
	ID     stream.Id       // Unique ID of the stream
	ctx    context.Context // to use for the operation

	registry Registry
	broker   Broker
	store    Store

	mu          sync.RWMutex // protects the fields below
	uncommitted []interface{}
	position    int64
}

// resets the stream object
func (s *Stream) Rollback() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.uncommitted = nil
	s.position = -1
	return nil
}

func (s *Stream) Commit() error {
	if len(s.uncommitted) == 0 {
		return nil
	}
	err := s.Begin() // make sure the position is updated
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer func() {
		s.position = -1
		s.uncommitted = nil
		s.mu.Unlock()
	}()

	tx, err := s.store.Begin(s.ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var records []record.Record
	var next = s.position
	for _, msg := range s.uncommitted {
		b, contentType, err := s.registry.Marshal(msg)
		if err != nil {
			return err
		}
		next = next + 1
		stored, err := s.store.StoreRecord(tx, s.ID, next, contentType, b)
		if err != nil {
			return err
		}
		records = append(records, stored)
	}
	err = tx.Commit()
	if err != nil {
		return err
	}

	err = s.broker.Notify(s.ctx, records...)
	if err != nil {
		return err
	}
	return nil
}

func (s *Stream) Size() (int64, error) {
	err := s.Begin()
	if err != nil {
		return -1, err
	}
	return s.position, nil
}

func (s *Stream) Append(messages ...interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.uncommitted = append(s.uncommitted, messages...)
}
func (s *Stream) AppendCommit(messages ...interface{}) error {
	s.Append(messages...)
	return s.Commit()
}

func (s *Stream) Project(projection stream.Handler) error {
	s.logger.Debugf("po/stream projecting %s", s.ID)
	s.mu.Lock()
	defer s.mu.Unlock()

	var position int64 = 0
	snap, supportsSnapshot := projection.(stream.NamedSnapshot)
	if supportsSnapshot {
		snapshot, err := s.store.ReadSnapshot(s.ctx, s.ID, snap.SnapshotName())
		if err != nil {
			return err
		}

		err = json.Unmarshal(snapshot.Data, projection)
		if err != nil {
			return err
		}
		position = snapshot.Position
	}

	records, err := s.store.ReadRecords(s.ctx, s.ID, position)
	if err != nil {
		return err
	}

	for _, r := range records {
		message, err := s.registry.ToMessage(r)
		if err != nil {
			return err
		}
		err = projection.Handle(s.ctx, message)
		if err != nil {
			return err
		}
		if s.ID.HasEntity() {
			position = message.Number
		} else {
			position = message.GroupNumber
		}

	}

	if supportsSnapshot {
		b, err := json.Marshal(projection)
		if err != nil {
			return err
		}

		err = s.store.UpdateSnapshot(s.ctx, s.ID, snap.SnapshotName(), record.Snapshot{
			Data:        b,
			Position:    position,
			ContentType: "application/json",
		})
		if err != nil {
			return err
		}
	}

	// store the position as the stream object is now considered active
	// This to guarantee users of the projection that messages appended
	// afterwards will be in the order their projection was made.
	s.position = position
	return nil
}

func (s *Stream) Execute(exec Executor) error {
	if handler, isHandler := exec.(stream.Handler); isHandler {
		err := s.Project(handler)
		if err != nil {
			return nil
		}
	}

	err := exec.Execute(s)
	if err != nil {
		return err
	}

	err = s.Commit()
	if err != nil {
		return err
	}
	return nil
}

// has side effect of updating the position
func (s *Stream) Begin() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.position >= 0 {
		return nil
	}
	position, err := s.store.GetStreamPosition(s.ctx, s.ID)
	if err != nil {
		return err
	}
	s.position = position
	return nil
}
