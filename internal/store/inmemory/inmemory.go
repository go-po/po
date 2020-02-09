package inmemory

import (
	"context"
	"fmt"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/store"
	"sync"
)

func New() *InMemory {
	return &InMemory{
		mu:   sync.RWMutex{},
		data: make(map[string][]record.Record),
	}
}

type InMemory struct {
	mu   sync.RWMutex               // guards the data
	data map[string][]record.Record // records by stream id
}

func (mem *InMemory) SequenceType(ctx context.Context, streamType string) (int64, error) {
	return 5, nil // TODO
}

func (mem *InMemory) Begin(ctx context.Context) (store.Tx, error) {
	return &inMemoryTx{
		records: make([]record.Record, 0),
		store:   mem,
	}, nil
}

func (mem *InMemory) Store(tx store.Tx, record record.Record) error {
	inTx, ok := tx.(*inMemoryTx)
	if !ok {
		return fmt.Errorf("unknown tx type: %T", tx)
	}
	inTx.records = append(inTx.records, record)

	return nil
}

func (mem *InMemory) ReadRecords(ctx context.Context, streamId string) ([]record.Record, error) {
	data, found := mem.data[streamId]
	if !found {
		return nil, nil
	}
	return data, nil
}

type inMemoryTx struct {
	records []record.Record
	store   *InMemory
}

func (tx inMemoryTx) Commit() error {
	tx.store.mu.Lock()
	defer tx.store.mu.Unlock()
	for _, r := range tx.records {
		_, hasStream := tx.store.data[r.Stream]
		if !hasStream {
			tx.store.data[r.Stream] = make([]record.Record, 0)
		}
		tx.store.data[r.Stream] = append(tx.store.data[r.Stream], r)
	}
	return nil
}

func (tx inMemoryTx) Rollback() error {
	return nil // discard the records in the transaction
}

var _ store.Tx = &inMemoryTx{}
