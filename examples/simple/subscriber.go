package simple

import (
	"context"
	"fmt"
	"github.com/go-po/po"
)

type Sub struct {
}

func (sub Sub) Handle(ctx context.Context, msg po.Message) error {
	switch message := msg.Data.(type) {
	case HelloMessage:
		fmt.Printf("[%d/%d] Greet: %s\n", msg.Number, msg.GroupNumber, message.Greeting)
	default:
		fmt.Printf("[%d/%d] Unknown type: %T\n", msg.Number, msg.GroupNumber, message)
	}
	return nil
}
