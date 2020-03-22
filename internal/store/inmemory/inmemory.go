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
		mu:        sync.RWMutex{},
		data:      make(map[string][]record.Record),
		ptr:       make(map[string]int64),
		snapshots: make(map[stream.Id]map[string]record.Snapshot),
	}
}

type InMemory struct {
	mu        sync.RWMutex               // guards the data
	data      map[string][]record.Record // records by stream group id
	ptr       map[string]int64           // subscriber positions
	snapshots map[stream.Id]map[string]record.Snapshot
}

var emptySnapshot = record.Snapshot{
	Data:        []byte("{}"),
	Position:    0,
	ContentType: "application/json",
}

func (mem *InMemory) ReadSnapshot(ctx context.Context, id stream.Id, snapshotId string) (record.Snapshot, error) {
	streamSnaps, found := mem.snapshots[id]
	if !found {
		return emptySnapshot, nil
	}

	snapshot, found := streamSnaps[snapshotId]
	if !found {
		fmt.Printf("didn't find it 2\n")
		return emptySnapshot, nil
	}
	return snapshot, nil

}

func (mem *InMemory) UpdateSnapshot(ctx context.Context, id stream.Id, snapshotId string, snapshot record.Snapshot) error {
	if _, found := mem.snapshots[id]; !found {
		mem.snapshots[id] = make(map[string]record.Snapshot)
	}
	mem.snapshots[id][snapshotId] = snapshot
	return nil
}

func (mem *InMemory) GetStreamPosition(ctx context.Context, id stream.Id) (int64, error) {
	mem.mu.Lock()
	defer mem.mu.Unlock()
	stream, found := mem.data[id.Group]
	if !found {
		return 0, nil
	}
	var position int64 = 0
	for _, r := range stream {
		if r.Stream.String() == id.String() {
			position = position + 1
		}
	}
	return position, nil
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
			if item.GroupNumber != 0 {
				return record.Record{}, fmt.Errorf("already assigned")
			}
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
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	data, found := mem.data[id.Group]
	if !found {
		return nil, nil
	}
	var result []record.Record
	for _, r := range data {
		if id.HasEntity() {
			if r.Stream.String() == id.String() && r.Number > from {
				result = append(result, r)
			}
		} else {
			if r.GroupNumber > from {
				result = append(result, r)
			}
		}
	}
	return result, nil
}

func (mem *InMemory) GetSubscriberPosition(tx store.Tx, subscriberId string, stream stream.Id) (int64, error) {
	mem.mu.RLock()
	defer mem.mu.RUnlock()
	pos, found := mem.ptr[subscriberId]
	if found {
		return pos, nil
	}
	return 0, nil
}

func (mem *InMemory) SetSubscriberPosition(tx store.Tx, subscriberId string, stream stream.Id, position int64) error {
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

	mem.mu.RLock()
	defer mem.mu.RUnlock()
	current, err := mem.ReadRecords(context.Background(), id, 0)
	if err != nil {
		return record.Record{}, err
	}
	var next = int64(len(current) + len(inTx.records) + 1)
	if next != number {
		return record.Record{}, fmt.Errorf("out of order: %d != %d", next, number)
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
