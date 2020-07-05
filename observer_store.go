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
		logger:         obs.Logger,
	}
}

type observesStore struct {
	store Store

	onWriteRecords binary.ClientTrace
	logger         observer.Logger
}

func (facade *observesStore) WriteRecords(ctx context.Context, id streams.Id, data ...record.Data) ([]record.Record, error) {
	done := facade.onWriteRecords.Observe(ctx, id.Group, strconv.Itoa(len(data)))
	defer done()
	records, err := facade.store.WriteRecords(ctx, id, data...)
	facade.logErr(err, "po/store write records: %s", err)
	return records, err
}

func (facade *observesStore) WriteRecordsFrom(ctx context.Context, id streams.Id, position int64, data ...record.Data) ([]record.Record, error) {
	records, err := facade.store.WriteRecordsFrom(ctx, id, position, data...)
	facade.logErr(err, "po/store write records: %s", err)
	return records, err
}

func (facade *observesStore) ReadSnapshot(ctx context.Context, id streams.Id, snapshotId string) (record.Snapshot, error) {
	snapshot, err := facade.store.ReadSnapshot(ctx, id, snapshotId)
	facade.logErr(err, "po/store read snapshot: %s", err)
	return snapshot, err
}

func (facade *observesStore) UpdateSnapshot(ctx context.Context, id streams.Id, snapshotId string, snapshot record.Snapshot) error {
	err := facade.store.UpdateSnapshot(ctx, id, snapshotId, snapshot)
	facade.logErr(err, "po/store update snapshot: %s", err)
	return err

}

func (facade *observesStore) Begin(ctx context.Context) (store.Tx, error) {
	tx, err := facade.store.Begin(ctx)
	facade.logErr(err, "po/store begin tx: %s", err)
	return tx, err
}

func (facade *observesStore) SubscriptionPositionLock(tx store.Tx, id streams.Id, subscriptionIds ...string) ([]store.SubscriptionPosition, error) {
	positions, err := facade.store.SubscriptionPositionLock(tx, id, subscriptionIds...)
	facade.logErr(err, "po/store lock subscription position: %s", err)
	return positions, err
}

func (facade *observesStore) ReadRecords(ctx context.Context, id streams.Id, from, to, limit int64) ([]record.Record, error) {
	rec, err := facade.store.ReadRecords(ctx, id, from, to, limit)
	facade.logErr(err, "po/store read records: %s", err)
	return rec, err
}

func (facade *observesStore) SetSubscriptionPosition(tx store.Tx, id streams.Id, position store.SubscriptionPosition) error {
	err := facade.store.SetSubscriptionPosition(tx, id, position)
	facade.logErr(err, "po/store set subscription position: %s", err)
	return err
}

func (facade *observesStore) logErr(err error, format string, args ...interface{}) {
	if err != nil {
		facade.logger.Errorf(format, args...)
	}
}
