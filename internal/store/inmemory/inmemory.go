package inmemory

import (
	"context"
	"fmt"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/internal/stream"
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
	data map[string][]record.Record // records by stream group id
}

func (mem *InMemory) AssignGroupNumber(ctx context.Context, r record.Record) (int64, error) {
	mem.mu.Lock()
	defer mem.mu.Unlock()
	id := stream.ParseId(r.Stream)
	groupData, found := mem.data[id.Group]
	if !found {
		return -1, fmt.Errorf("unknown stream group: %s", id.Group)
	}
	for i, item := range groupData {
		if item.Stream == r.Stream && item.Number == r.Number {
			groupNumber := int64(i) + 1
			r := groupData[i]
			r.GroupNumber = groupNumber
			groupData[i] = r
			return groupNumber, nil
		}
	}
	return -1, fmt.Errorf("number %d not found in stream %s", r.Number, id.Group)

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
	id := stream.ParseId(streamId)
	data, found := mem.data[id.Group]
	if !found {
		return nil, nil
	}
	var result []record.Record
	for _, r := range data {
		if r.Stream == streamId {
			result = append(result, r)
		}
	}
	return result, nil
}

type inMemoryTx struct {
	records []record.Record
	store   *InMemory
}

func (tx inMemoryTx) Commit() error {
	tx.store.mu.Lock()
	defer tx.store.mu.Unlock()
	for _, r := range tx.records {
		streamId := stream.ParseId(r.Stream)
		_, hasStream := tx.store.data[streamId.Group]
		if !hasStream {
			tx.store.data[streamId.Group] = make([]record.Record, 0)
		}
		tx.store.data[streamId.Group] = append(tx.store.data[streamId.Group], r)
	}
	return nil
}

func (tx inMemoryTx) Rollback() error {
	return nil // discard the records in the transaction
}

var _ store.Tx = &inMemoryTx{}
