package broker

import (
	"context"

	"github.com/go-po/po/streams"
)

type Observer interface {
	Register(ctx context.Context, id streams.Id, subscriberId string) func()
	Notify(ctx context.Context, count int64) func()
}
