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
		fmt.Printf("[%d] Greet: %s\n", msg.Id, message.Greeting)
	default:
		fmt.Printf("[%d] Unknown type: %T\n", msg.Id, message)
	}
	return nil
}
