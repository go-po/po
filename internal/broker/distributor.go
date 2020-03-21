package broker

import (
	"context"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/stream"
)

type Distributor interface {
	Distribute(ctx context.Context, record record.Record) (bool, error)
	Register(ctx context.Context, subscriberId string, streamId stream.Id, subscriber interface{}) error
}

type GroupAssigner interface {
	AssignGroup(ctx context.Context, id stream.Id, number int64) (record.Record, error)
}
