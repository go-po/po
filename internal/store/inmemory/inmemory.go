package inmemory

import (
	"context"
	"fmt"
	"github.com/kyuff/po/internal/store"
	"sync"
)

func New() *InMemory {
	return &InMemory{
		mu:   sync.RWMutex{},
		data: make(map[string][]store.Record),
	}
}

type InMemory struct {
	mu   sync.RWMutex              // guards the data
	data map[string][]store.Record // records by stream id
}

func (mem *InMemory) Begin(ctx context.Context) (store.Tx, error) {
	return &inMemoryTx{
		records: make([]store.Record, 0),
		store:   mem,
	}, nil
}

func (mem *InMemory) Store(tx store.Tx, record store.Record) error {
	inTx, ok := tx.(*inMemoryTx)
	if !ok {
		return fmt.Errorf("unknown tx type: %T", tx)
	}
	inTx.records = append(inTx.records, record)

	return nil
}

func (mem *InMemory) ReadRecords(ctx context.Context, streamId string) ([]store.Record, error) {
	data, found := mem.data[streamId]
	if !found {
		return nil, nil
	}
	return data, nil
}

type inMemoryTx struct {
	records []store.Record
	store   *InMemory
}

func (tx inMemoryTx) Commit() error {
	tx.store.mu.Lock()
	defer tx.store.mu.Unlock()
	for _, record := range tx.records {
		_, hasStream := tx.store.data[record.Stream]
		if !hasStream {
			tx.store.data[record.Stream] = make([]store.Record, 0)
		}
		tx.store.data[record.Stream] = append(tx.store.data[record.Stream], record)
	}
	return nil
}

func (tx inMemoryTx) Rollback() error {
	return nil // discard the records in the transaction
}

var _ store.Tx = &inMemoryTx{}
