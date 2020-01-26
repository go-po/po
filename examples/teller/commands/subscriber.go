package commands

import (
	"context"
	"github.com/kyuff/po"
	"log"
)

func NewCommandSubscriber() *Subscriber {
	return &Subscriber{}
}

type Subscriber struct {
}

func (handler *Subscriber) Handle(ctx context.Context, msg po.Message) error {
	log.Printf("Message received: %T", msg.Data)
	return nil
}
