package commands

import (
	"context"
	"github.com/kyuff/po"
	"github.com/kyuff/po/examples/teller/events"
	"log"
)

func NewCommandSubscriber(po *po.Po) *Subscriber {
	return &Subscriber{
		po: po,
	}
}

type Subscriber struct {
	po *po.Po
}

func (sub *Subscriber) Handle(ctx context.Context, msg po.Message) error {
	log.Printf("Message %s.%s.%d", msg.Stream, msg.Type, msg.Id)
	switch cmd := msg.Data.(type) {
	case DeclareCommand:
		streamId := "vars-" + cmd.Name
		stream := sub.po.Stream(ctx, streamId)
		size, err := stream.Size()
		if err != nil {
			return err
		}
		if size != 0 {
			return nil // idempotence, already declared
		}
		return stream.Append(events.DeclaredEvent{Name: cmd.Name})
	case AddCommand:
		return sub.po.Stream(ctx, "vars-"+cmd.Name).
			Append(events.AddedEvent{Value: cmd.Number})
	case SubCommand:
		return sub.po.Stream(ctx, "vars-"+cmd.Name).
			Append(events.SubtractedEvent{Value: cmd.Number})
	default:
		// nothing to do
	}
	return nil
}
