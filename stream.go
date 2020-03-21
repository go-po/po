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

func (stream *Stream) Begin() (*Tx, error) {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if stream.tx == nil {
		position, err := stream.store.GetStreamPosition(stream.ctx, stream.ID)
		if err != nil {
			return nil, err
		}
		stream.tx = &Tx{
			position: position,
			stream:   stream,
		}
	}
	return stream.tx, nil
}

func (stream *Stream) AppendTx(tx *Tx, messages ...interface{}) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.uncommitted = append(tx.uncommitted, messages...)
}

func (stream *Stream) Append(messages ...interface{}) error {
	if len(messages) == 0 {
		return nil // nothing to do
	}

	tx, err := stream.Begin()
	if err != nil {
		return err
	}
	stream.AppendTx(tx, messages...)

	return tx.Commit()
}

// Projects the stream onto the provided projection.
// Doing so starts an implicit transaction,
// so that Appends will join the transaction
func (stream *Stream) Project(projection interface{}) error {
	_, err := stream.Begin()
	if err != nil {
		return err
	}

	return nil
}
