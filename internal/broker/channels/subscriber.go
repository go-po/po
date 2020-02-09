package channels

import (
	"context"
	"github.com/go-po/po"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/registry"
)

func newSubscriber(seq TypeSequencer) *subscriber {
	return &subscriber{
		seq: seq,
	}
}

type TypeSequencer interface {
	SequenceType(ctx context.Context, streamType string) (int64, error)
}

type subscriber struct {
	seq TypeSequencer
}

func (sub *subscriber) addInbound(ctx context.Context, streamId po.StreamId, ch <-chan record.Record, h interface{}) error {
	handler, err := wrapSubscriber(h)
	if err != nil {
		return err
	}
	go func() {
		_ = sub.handle(ctx, streamId, ch, handler)

	}()
	return nil
}

func (sub *subscriber) handle(subCtx context.Context, streamId po.StreamId, ch <-chan record.Record, handler po.Handler) error {
	for {
		select {
		case <-subCtx.Done():
			return nil
		case rec := <-ch:
			ctx := context.Background()
			incStreamId := po.ParseStreamId(rec.Stream)
			seq, err := sub.seq.SequenceType(ctx, incStreamId.Group)
			if err != nil {
				// TODO make an err channel for this
				// for now, ignore the err
			}
			rec.GroupNumber = seq
			err = sub.tryRecord(streamId, rec, incStreamId, handler)
			if err != nil {
				// TODO make an err channel for this
				// for now, ignore the err
			}
		}
	}
}

func (sub *subscriber) tryRecord(streamId po.StreamId, rec record.Record, incStreamId po.StreamId, handler po.Handler) error {
	msg, err := po.ToMessage(registry.DefaultRegistry, rec)
	if err != nil {
		return err
	}
	if streamId.HasEntity() {
		if streamId.Entity == incStreamId.Entity {
			return handler.Handle(context.Background(), msg)
		} else {
			// discard
			return nil
		}
	} else {
		return handler.Handle(context.Background(), msg)
	}
}
