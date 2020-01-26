package channels

import (
	"context"
	"fmt"
	"github.com/kyuff/po"
	"github.com/kyuff/po/internal/store"
	"log"
	"sync"
)

func New() *Channels {
	n := &Channels{
		comm: make(chan store.Record),
		subs: make(map[string][]po.Handler),
	}
	n.Start()
	return n
}

type Channels struct {
	comm chan store.Record

	mu   sync.Mutex // protects below
	subs map[string][]po.Handler
}

var _ po.Broker = &Channels{}

func (ch *Channels) Notify(ctx context.Context, records ...store.Record) error {
	go func() {
		for _, record := range records {
			ch.comm <- record
		}
	}()
	return nil
}

func (ch *Channels) Subscribe(ctx context.Context, subscriptionId, streamId string, subscriber interface{}) error {
	handler, err := wrapSubscriber(subscriber)
	if err != nil {
		return err
	}
	ch.mu.Lock()
	defer ch.mu.Unlock()

	_, ok := ch.subs[streamId]
	if !ok {
		ch.subs[streamId] = make([]po.Handler, 0)
	}
	ch.subs[streamId] = append(ch.subs[streamId], handler)
	return nil
}

func wrapSubscriber(subscriber interface{}) (po.Handler, error) {
	switch h := subscriber.(type) {
	case po.Handler:
		return h, nil
	default:
		return nil, fmt.Errorf("no way to handle")
	}
}

func (ch *Channels) Start() {
	go ch.listen()
}

func (ch *Channels) Stop() {

}

func (ch *Channels) listen() {
	log.Printf("Started listening")
	for record := range ch.comm {
		subs, found := ch.subs[record.Stream]
		if !found {
			return
		}
		data, err := po.LookupData(record.Type, record.Data)
		if err != nil {
			log.Printf("notify failed data: %s", err)
			continue
		}
		msg := po.Message{
			Stream: record.Stream,
			Data:   data,
			Type:   record.Type,
		}
		for _, sub := range subs {
			err := sub.Handle(context.Background(), msg)
			if err != nil {
				log.Printf("notify failed handle: %s", err)
			}
		}

	}
}
