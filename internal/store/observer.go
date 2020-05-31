package store

import (
	"github.com/go-po/po/internal/observer"
	"github.com/go-po/po/internal/observer/binary"
	"github.com/go-po/po/internal/observer/unary"
	"github.com/prometheus/client_golang/prometheus"
)

type Observer struct {
	// a=snapshot, b=group
	ReadSnapshot binary.ClientTrace
	// a=snapshot, b=group
	UpdateSnapshot binary.ClientTrace
	// a=group, b=stream
	GetStreamPosition binary.ClientTrace
	// a=group, b=stream
	ReadRecords binary.ClientTrace
	// a=group, b=subscriberId
	GetSubscriberPosition binary.ClientTrace
	// a=group, b=subscriberId
	SetSubscriberPosition binary.ClientTrace
	// a=group
	StoreRecord unary.ClientTrace
	// a=group
	AssignGroup unary.ClientTrace
}

func DefaultObserver(builder *observer.Builder, name string) Observer {
	return Observer{
		ReadSnapshot: builder.Binary().
			LogDebugf("%s read snapshot %s from group %s", name).
			MetricCounter(prometheus.NewCounterVec(prometheus.CounterOpts{
				Name: "po_store_snapshots_read_counter",
				Help: "Number of times a snapshot is read",
			}, []string{"snapshot", "group"})).
			Build(),
		UpdateSnapshot: builder.Binary().
			LogDebugf("%s update snapshot %s from group %s", name).
			Build(),
		GetStreamPosition:     builder.Binary().LogDebugf("%s get stream position %s", name).Build(),
		ReadRecords:           builder.Binary().LogDebugf("%s read records %s %s", name).Build(),
		GetSubscriberPosition: builder.Binary().LogDebugf("%s get sub pos from group with sub %s", name).Build(),
		SetSubscriberPosition: builder.Binary().LogDebugf("%s set sub pos on group %s with sub %s", name).Build(),
		StoreRecord:           builder.Unary().LogDebugf("%s store record in group %s", name).Build(),
		AssignGroup:           builder.Unary().LogDebugf("%s assign group to %s", name).Build(),
	}
}

func StubObserver() Observer {
	return Observer{
		ReadSnapshot:          binary.Noop(),
		UpdateSnapshot:        binary.Noop(),
		GetStreamPosition:     binary.Noop(),
		ReadRecords:           binary.Noop(),
		GetSubscriberPosition: binary.Noop(),
		SetSubscriberPosition: binary.Noop(),
		StoreRecord:           unary.Noop(),
		AssignGroup:           unary.Noop(),
	}
}
