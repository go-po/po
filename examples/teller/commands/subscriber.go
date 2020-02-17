package commands

import (
	"context"
	"github.com/go-po/po"
	"github.com/go-po/po/examples/teller/events"
	"github.com/go-po/po/stream"
)

func NewCommandSubscriber(dao *po.Po) *CmdSub {
	return &CmdSub{
		dao: dao,
	}
}

type CmdSub struct {
	dao *po.Po
}

func (sub *CmdSub) Handle(ctx context.Context, msg stream.Message) error {
	switch cmd := msg.Data.(type) {
	case DeclareCommand:
		streamId := "vars-" + cmd.Name
		stream := sub.dao.Stream(ctx, streamId)
		size, err := stream.Size()
		if err != nil {
			return err
		}
		if size != 0 {
			return nil // idempotence, already declared
		}
		return stream.Append(events.DeclaredEvent{Name: cmd.Name})
	case AddCommand:
		return sub.dao.Stream(ctx, "vars-"+cmd.Name).
			Append(events.AddedEvent{Value: cmd.Number})
	case SubCommand:
		return sub.dao.Stream(ctx, "vars-"+cmd.Name).
			Append(events.SubtractedEvent{Value: cmd.Number})
	default:
		// nothing to do
	}
	return nil
}
