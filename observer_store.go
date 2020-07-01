package po

import (
	"context"
	"strconv"

	"github.com/go-po/po/internal/observer"
	"github.com/go-po/po/internal/observer/binary"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/streams"
)

func observeStore(store Store, obs *observer.Builder) *observesStore {
	return &observesStore{
		store:          store,
		onWriteRecords: obs.Binary().Build(),
	}
}

type observesStore struct {
	store Store

	onWriteRecords binary.ClientTrace
}

func (facade *observesStore) WriteRecords(ctx context.Context, id streams.Id, data ...record.Data) ([]record.Record, error) {
	done := facade.onWriteRecords.Observe(ctx, id.Group, strconv.Itoa(len(data)))
	defer done()
	return facade.store.WriteRecords(ctx, id, data...)
}

func (facade *observesStore) WriteRecordsFrom(ctx context.Context, id streams.Id, position int64, data ...record.Data) ([]record.Record, error) {
	return facade.store.WriteRecordsFrom(ctx, id, position, data...)
}

func (facade *observesStore) ReadSnapshot(ctx context.Context, id streams.Id, snapshotId string) (record.Snapshot, error) {
	return facade.store.ReadSnapshot(ctx, id, snapshotId)
}

func (facade *observesStore) UpdateSnapshot(ctx context.Context, id streams.Id, snapshotId string, snapshot record.Snapshot) error {
	return facade.store.UpdateSnapshot(ctx, id, snapshotId, snapshot)
}

func (facade *observesStore) Begin(ctx context.Context) (store.Tx, error) {
	return facade.store.Begin(ctx)
}

func (facade *observesStore) SubscriptionPositionLock(tx store.Tx, id streams.Id, subscriptionIds ...string) ([]store.SubscriptionPosition, error) {
	return facade.store.SubscriptionPositionLock(tx, id, subscriptionIds...)
}

func (facade *observesStore) ReadRecords(ctx context.Context, id streams.Id, from, to, limit int64) ([]record.Record, error) {
	return facade.store.ReadRecords(ctx, id, from, to, limit)
}

func (facade *observesStore) SetSubscriptionPosition(tx store.Tx, id streams.Id, position store.SubscriptionPosition) error {
	return facade.store.SetSubscriptionPosition(tx, id, position)
}
