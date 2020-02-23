package inmemory

import (
	"context"
	"fmt"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/stream"
	"sync"
	"time"
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
	groupData, found := mem.data[r.Stream.Group]
	if !found {
		return -1, fmt.Errorf("unknown stream group: %s", r.Stream.Group)
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
	return -1, fmt.Errorf("number %d not found in stream %s", r.Number, r.Stream.Group)

}

func (mem *InMemory) Begin(ctx context.Context) (store.Tx, error) {
	return &inMemoryTx{
		records: make([]record.Record, 0),
		store:   mem,
	}, nil
}

func (mem *InMemory) StoreRecord(tx store.Tx, id stream.Id, msgType string, data []byte) (record.Record, error) {
	inTx, ok := tx.(*inMemoryTx)
	if !ok {
		return record.Record{}, fmt.Errorf("unknown tx type: %T", tx)
	}

	current, err := mem.ReadRecords(context.Background(), id)
	if err != nil {
		return record.Record{}, err
	}
	number := int64(len(current) + len(inTx.records))
	r := record.Record{
		Number:      number,
		Stream:      id,
		Data:        data,
		Group:       msgType,
		GroupNumber: 0,
		Time:        time.Now(),
	}
	inTx.records = append(inTx.records, r)
	return r, nil
}

func (mem *InMemory) ReadRecords(ctx context.Context, id stream.Id) ([]record.Record, error) {
	data, found := mem.data[id.Group]
	if !found {
		return nil, nil
	}
	var result []record.Record
	for _, r := range data {
		if r.Stream.String() == id.String() {
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

		_, hasStream := tx.store.data[r.Stream.Group]
		if !hasStream {
			tx.store.data[r.Stream.Group] = make([]record.Record, 0)
		}
		tx.store.data[r.Stream.Group] = append(tx.store.data[r.Stream.Group], r)
	}
	return nil
}

func (tx inMemoryTx) Rollback() error {
	return nil // discard the records in the transaction
}

var _ store.Tx = &inMemoryTx{}
