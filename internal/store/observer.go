package store

import (
	"context"

	"github.com/go-po/po/internal/observer"
	"github.com/go-po/po/internal/observer/binary"
	"github.com/go-po/po/internal/observer/unary"
	"github.com/go-po/po/streams"
	"github.com/prometheus/client_golang/prometheus"
)

type Observer interface {
	ReadSnapshot(ctx context.Context, id streams.Id, snapshotId string) func()
	UpdateSnapshot(ctx context.Context, id streams.Id, snapshotId string) func()
	GetStreamPosition(ctx context.Context, id streams.Id) func()
	ReadRecords(ctx context.Context, id streams.Id) func()
	GetSubscriberPosition(ctx context.Context, id streams.Id, subscriberId string) func()
	SetSubscriberPosition(ctx context.Context, id streams.Id, subscriberId string) func()
	StoreRecord(ctx context.Context, id streams.Id) func()
	AssignGroup(ctx context.Context, id streams.Id) func()
}

func DefaultObserver(builder *observer.Builder, name string) *obs {
	return &obs{
		readSnapshot: builder.Binary().
			LogDebugf("%s read snapshot %s from group %s", name).
			MetricCounter(prometheus.NewCounterVec(prometheus.CounterOpts{
				Name: "po_store_snapshots_read_counter",
				Help: "Number of times a snapshot is read",
			}, []string{"snapshot", "group"})).
			Build(),
		updateSnapshot:        binary.Noop(),
		getStreamPosition:     binary.Noop(),
		readRecords:           binary.Noop(),
		getSubscriberPosition: binary.Noop(),
		setSubscriberPosition: binary.Noop(),
		storeRecord:           unary.Noop(),
		assignGroup:           builder.Unary().LogDebugf("%s assign group to %s", name).Build(),
	}

}

func StubObserver() *obs {
	return &obs{
		readSnapshot:          binary.Noop(),
		updateSnapshot:        binary.Noop(),
		getStreamPosition:     binary.Noop(),
		readRecords:           binary.Noop(),
		getSubscriberPosition: binary.Noop(),
		setSubscriberPosition: binary.Noop(),
		storeRecord:           unary.Noop(),
		assignGroup:           unary.Noop(),
	}
}

var _ Observer = &obs{}

type obs struct {
	readSnapshot          binary.ClientTrace
	updateSnapshot        binary.ClientTrace
	getStreamPosition     binary.ClientTrace
	readRecords           binary.ClientTrace
	getSubscriberPosition binary.ClientTrace
	setSubscriberPosition binary.ClientTrace
	storeRecord           unary.ClientTrace
	assignGroup           unary.ClientTrace
}

func (o *obs) ReadSnapshot(ctx context.Context, id streams.Id, snapshotId string) func() {
	return o.readSnapshot.Observe(ctx, id.Group, snapshotId)
}

func (o *obs) UpdateSnapshot(ctx context.Context, id streams.Id, snapshotId string) func() {
	return o.updateSnapshot.Observe(ctx, id.Group, snapshotId)
}

func (o *obs) GetStreamPosition(ctx context.Context, id streams.Id) func() {
	return o.getStreamPosition.Observe(ctx, id.Group, id.String())
}

func (o *obs) ReadRecords(ctx context.Context, id streams.Id) func() {
	return o.readRecords.Observe(ctx, id.Group, id.String())
}

func (o *obs) GetSubscriberPosition(ctx context.Context, id streams.Id, subscriberId string) func() {
	return o.getSubscriberPosition.Observe(ctx, id.Group, subscriberId)
}

func (o *obs) SetSubscriberPosition(ctx context.Context, id streams.Id, subscriberId string) func() {
	return o.setSubscriberPosition.Observe(ctx, id.Group, subscriberId)
}

func (o *obs) StoreRecord(ctx context.Context, id streams.Id) func() {
	return o.storeRecord.Observe(ctx, id.Group)
}

func (o *obs) AssignGroup(ctx context.Context, id streams.Id) func() {
	return o.assignGroup.Observe(ctx, id.Group)
}
