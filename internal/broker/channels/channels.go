package channels

import (
	"context"

	"github.com/go-po/po/internal/broker"
	"github.com/go-po/po/internal/record"
)

func New() *Channels {
	return &Channels{}
}

type Channels struct{}

func (ch *Channels) Register(ctx context.Context, group string, input broker.RecordHandler) (broker.RecordHandler, error) {
	return broker.RecordHandlerFunc(func(ctx context.Context, record record.Record) (bool, error) {
		return input.Handle(ctx, record)
	}), nil
}
