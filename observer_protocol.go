package po

import (
	"context"

	"github.com/go-po/po/internal/broker"
	"github.com/go-po/po/internal/observer"
	"github.com/go-po/po/internal/observer/unary"
	"github.com/go-po/po/internal/record"
)

func observeProtocol(protocol broker.Protocol, builder *observer.Builder) *observesProtocol {
	return &observesProtocol{
		protocol: protocol,
		//onIn:     builder.Unary().LogDebugf("po/protocol in-bound: %s").Build(),
		onIn: builder.Unary().Build(),
		//onOut:    builder.Unary().LogDebugf("po/protocol out-bound: %s").Build(),
		onOut: builder.Unary().Build(),
	}
}

type observesProtocol struct {
	protocol broker.Protocol

	onIn  unary.ClientTrace
	onOut unary.ClientTrace
}

func (obs *observesProtocol) Register(ctx context.Context, group string, input broker.RecordHandler) (broker.RecordHandler, error) {
	publisher, err := obs.protocol.Register(ctx, group, broker.RecordHandlerFunc(func(ctx context.Context, record record.Record) (bool, error) {
		// observe in-bound
		done := obs.onIn.Observe(ctx, record.Group)
		defer done()
		return input.Handle(ctx, record)
	}))
	if err != nil {
		return publisher, err
	}
	return broker.RecordHandlerFunc(func(ctx context.Context, record record.Record) (bool, error) {
		// observe out-bound
		done := obs.onOut.Observe(ctx, record.Group)
		defer done()
		return publisher.Handle(ctx, record)
	}), nil
}
