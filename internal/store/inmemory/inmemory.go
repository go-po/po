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
		ptr:  make(map[string]int64),
	}
}

type InMemory struct {
	mu   sync.RWMutex               // guards the data
	data map[string][]record.Record // records by stream group id
	ptr  map[string]int64           // subscriber positions
}

func (mem *InMemory) AssignGroup(ctx context.Context, id stream.Id, number int64) (record.Record, error) {
	mem.mu.Lock()
	defer mem.mu.Unlock()
	groupData, found := mem.data[id.Group]
	if !found {
		return record.Record{}, fmt.Errorf("unknown stream group: %s", id.Group)
	}
	for i, item := range groupData {
		if item.Stream.String() == id.String() && item.Number == number {
			groupNumber := int64(i) + 1
			r := groupData[i]
			r.GroupNumber = groupNumber
			groupData[i] = r
			return r, nil
		}
	}
	return record.Record{}, fmt.Errorf("number %d not found in stream %s", number, id.Group)
}

func (mem *InMemory) ReadRecords(ctx context.Context, id stream.Id, from int64) ([]record.Record, error) {
	data, found := mem.data[id.Group]
	if !found {
		return nil, nil
	}
	var result []record.Record
	for _, r := range data {
		if r.Stream.String() == id.String() && r.Number > from {
			result = append(result, r)
		}
	}
	return result, nil
}

func (mem *InMemory) GetLastPosition(tx store.Tx, subscriberId string, stream stream.Id) (int64, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	pos, found := mem.ptr[subscriberId]
	if found {
		return pos, nil
	}
	return 0, nil
}

func (mem *InMemory) SetPosition(tx store.Tx, subscriberId string, stream stream.Id, position int64) error {
	mem.mu.Lock()
	defer mem.mu.Unlock()
	mem.ptr[subscriberId] = position
	return nil
}

func (mem *InMemory) Begin(ctx context.Context) (store.Tx, error) {
	return &inMemoryTx{
		records: make([]record.Record, 0),
		store:   mem,
	}, nil
}

func (mem *InMemory) StoreRecord(tx store.Tx, id stream.Id, number int64, msgType string, data []byte) (record.Record, error) {
	inTx, ok := tx.(*inMemoryTx)
	if !ok {
		return record.Record{}, fmt.Errorf("unknown tx type: %T", tx)
	}

	r := record.Record{
		Number:      number,
		Stream:      id,
		Data:        data,
		Group:       id.Group,
		ContentType: msgType,
		GroupNumber: 0,
		Time:        time.Now(),
	}
	inTx.records = append(inTx.records, r)
	return r, nil
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
