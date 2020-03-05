package channels

import (
	"context"
	"github.com/go-po/po/internal/broker"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/stream"
)

func newSubscriber(groupNumbers broker.GroupAssigner) *subscriber {
	return &subscriber{
		groupNumbers: groupNumbers,
	}
}

type subscriber struct {
	groupNumbers broker.GroupAssigner
	dist         broker.Distributor
}

func (sub *subscriber) addInbound(ctx context.Context, streamId stream.Id, ch <-chan record.Record) error {
	go func() {
		_ = sub.handle(ctx, streamId, ch)

	}()
	return nil
}

func (sub *subscriber) handle(subCtx context.Context, streamId stream.Id, ch <-chan record.Record) error {
	for {
		select {
		case <-subCtx.Done():
			return nil
		case rec := <-ch:
			_, err := sub.dist.Distribute(context.Background(), rec)
			if err != nil {
				// TODO make an err channel for this
				// for now, ignore the err
			}

			//
			//ctx := context.Background()
			//incStreamId := po.ParseStreamId(rec.Stream)
			//groupNumber, err := sub.groupNumbers.AssignGroupNumber(ctx, rec)
			//if err != nil {
			//	// TODO make an err channel for this
			//	// for now, ignore the err
			//}
			//rec.GroupNumber = groupNumber
			//err = sub.tryRecord(streamId, rec, incStreamId, handler)
			//if err != nil {
			//	// TODO make an err channel for this
			//	// for now, ignore the err
			//}
		}
	}
}

//func (sub *subscriber) tryRecord(streamId po.StreamId, rec record.Record, incStreamId po.StreamId, handler po.Handler) error {
//	msg, err := po.ToMessage(registry.DefaultRegistry, rec)
//	if err != nil {
//		return err
//	}
//	if streamId.HasEntity() {
//		if streamId.Entity == incStreamId.Entity {
//			return handler.Handle(context.Background(), msg)
//		} else {
//			// discard
//			return nil
//		}
//	} else {
//		return handler.Handle(context.Background(), msg)
//	}
//}
