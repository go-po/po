package po

import (
	"context"

	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/streams"
)

func observeStore(store Store) *storeObserver {
	return &storeObserver{store: store}
}

type storeObserver struct {
	store Store
}

func (obs *storeObserver) WriteRecords(ctx context.Context, id streams.Id, data ...record.Data) ([]record.Record, error) {
	return obs.store.WriteRecords(ctx, id, data...)
}

func (obs *storeObserver) WriteRecordsFrom(ctx context.Context, id streams.Id, position int64, data ...record.Data) ([]record.Record, error) {
	return obs.store.WriteRecordsFrom(ctx, id, position, data...)
}

func (obs *storeObserver) ReadSnapshot(ctx context.Context, id streams.Id, snapshotId string) (record.Snapshot, error) {
	return obs.store.ReadSnapshot(ctx, id, snapshotId)
}

func (obs *storeObserver) UpdateSnapshot(ctx context.Context, id streams.Id, snapshotId string, snapshot record.Snapshot) error {
	return obs.store.UpdateSnapshot(ctx, id, snapshotId, snapshot)
}

func (obs *storeObserver) Begin(ctx context.Context) (store.Tx, error) {
	return obs.store.Begin(ctx)
}

func (obs *storeObserver) SubscriptionPositionLock(tx store.Tx, id streams.Id, subscriptionIds ...string) ([]store.SubscriptionPosition, error) {
	return obs.store.SubscriptionPositionLock(tx, id, subscriptionIds...)
}

func (obs *storeObserver) ReadRecords(ctx context.Context, id streams.Id, from, to, limit int64) ([]record.Record, error) {
	return obs.store.ReadRecords(ctx, id, from, to, limit)
}

func (obs *storeObserver) SetSubscriptionPosition(tx store.Tx, id streams.Id, position store.SubscriptionPosition) error {
	return obs.store.SetSubscriptionPosition(tx, id, position)
}
