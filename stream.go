package po

import (
	"context"
	"encoding/json"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/stream"
	"sync"
)

type Appender interface {
	Append(message ...interface{}) error
}

type TxAppender interface {
	Begin() (*Tx, error)
	AppendTx(tx *Tx, message ...interface{})
}

type Tx struct {
	stream *Stream

	mu          sync.RWMutex  // protects below field
	uncommitted []interface{} // messages to be appended
	position    int64         // last committed position
}

func (tx *Tx) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	storeTx, err := tx.stream.store.Begin(tx.stream.ctx)
	if err != nil {
		return err
	}
	defer func() { _ = storeTx.Rollback() }()

	var records []record.Record
	var next = tx.position
	for _, msg := range tx.uncommitted {
		b, contentType, err := tx.stream.registry.Marshal(msg)
		if err != nil {
			return err
		}
		next = next + 1
		stored, err := tx.stream.store.StoreRecord(storeTx, tx.stream.ID, next, contentType, b)
		if err != nil {
			return err
		}
		records = append(records, stored)
	}
	err = storeTx.Commit()
	if err != nil {
		return err
	}
	tx.uncommitted = nil
	tx.stream.tx = nil // delete own reference
	tx.position = next

	return tx.stream.broker.Notify(tx.stream.ctx, records...)
}

func (tx *Tx) Rollback() error {
	tx.uncommitted = nil
	tx.stream.tx = nil // delete own reference
	return nil
}

type Stream struct {
	ID  stream.Id       // Unique ID of the stream
	ctx context.Context // to use for the operation
	mu  sync.RWMutex    // protects tx
	tx  *Tx

	registry Registry
	broker   Broker
	store    Store
}

var _ Appender = &Stream{}
var _ TxAppender = &Stream{}

func (s *Stream) Begin() (*Tx, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tx == nil {
		position, err := s.store.GetStreamPosition(s.ctx, s.ID)
		if err != nil {
			return nil, err
		}
		s.tx = &Tx{
			position: position,
			stream:   s,
		}
	}
	return s.tx, nil
}

func (s *Stream) AppendTx(tx *Tx, messages ...interface{}) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.uncommitted = append(tx.uncommitted, messages...)
}

func (s *Stream) Append(messages ...interface{}) error {
	if len(messages) == 0 {
		return nil // nothing to do
	}

	tx, err := s.Begin()
	if err != nil {
		return err
	}
	s.AppendTx(tx, messages...)

	return tx.Commit()
}

// Number of messages in a Stream
func (s *Stream) Size() (int64, error) {
	return s.store.GetStreamPosition(s.ctx, s.ID)
}

// Projects the stream onto the provided projection.
// Doing so starts an implicit transaction,
// so that Appends will join the transaction
func (s *Stream) Project(projection stream.Handler) error {
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

	s.tx = &Tx{
		position: position,
		stream:   s,
	}

	return nil
}
