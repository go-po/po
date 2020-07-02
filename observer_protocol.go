package po

import (
	"context"

	"github.com/go-po/po/internal/broker"
	"github.com/go-po/po/internal/observer"
	"github.com/go-po/po/internal/record"
)

func observeProtocol(protocol broker.Protocol, builder *observer.Builder) *observesProtocol {
	return &observesProtocol{
		protocol: protocol,
	}
}

type observesProtocol struct {
	protocol broker.Protocol
}

func (obs *observesProtocol) Register(ctx context.Context, group string, input broker.RecordHandler) (broker.RecordHandler, error) {
	publisher, err := obs.protocol.Register(ctx, group, broker.RecordHandlerFunc(func(ctx context.Context, record record.Record) (bool, error) {
		// observe in-bound
		return input.Handle(ctx, record)
	}))
	if err != nil {
		return publisher, err
	}
	return broker.RecordHandlerFunc(func(ctx context.Context, record record.Record) (bool, error) {
		// observe out-bound
		return publisher.Handle(ctx, record)
	}), nil
}
