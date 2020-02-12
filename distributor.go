package po

import (
	"context"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/stream"
)

func newDistributor() *distributor {
	return &distributor{}
}

type distributor struct {
}

func (dist *distributor) Register(ctx context.Context, subscriberId string, streamId stream.Id, subscriber interface{}) error {
	return nil
}

func (dist *distributor) Distribute(ctx context.Context, record record.Record) error {
	return nil
}
