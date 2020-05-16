package broker

import (
	"context"

	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/streams"
)

type Distributor interface {
	Distribute(ctx context.Context, record record.Record) (bool, error)
	Register(ctx context.Context, subscriberId string, streamId streams.Id, subscriber interface{}) error
}

type GroupAssigner interface {
	AssignGroup(ctx context.Context, id streams.Id, number int64) (record.Record, error)
}
