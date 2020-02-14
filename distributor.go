package po

import (
	"context"
	"fmt"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/registry"
	"github.com/go-po/po/internal/stream"
	"sync"
)

func newDistributor(groupNumbers groupNumberAssigner) *distributor {
	return &distributor{
		subs:         make(map[string][]Handler),
		groupNumbers: groupNumbers,
	}
}

type groupNumberAssigner interface {
	AssignGroupNumber(ctx context.Context, r record.Record) (int64, error)
}

type distributor struct {
	mu           sync.Mutex           // guards below maps
	subs         map[string][]Handler // stream group to handler
	groupNumbers groupNumberAssigner
}

func (dist *distributor) Register(ctx context.Context, subscriberId string, stream stream.Id, subscriber interface{}) error {
	dist.mu.Lock()
	defer dist.mu.Unlock()
	handler, err := wrapSubscriber(subscriber)
	if err != nil {
		return err
	}
	dist.subs[stream.Group] = append(dist.subs[stream.Group], handler)

	return nil
}

func (dist *distributor) Distribute(ctx context.Context, record record.Record) (bool, error) {
	stream := stream.ParseId(record.Stream)
	subs, hasSubs := dist.subs[stream.Group]
	if !hasSubs {
		return false, nil
	}
	var msg Message
	msg, err := ToMessage(registry.DefaultRegistry, record)
	if err != nil {
		// TODO faulty implementation, catch later
		return false, err
	}
	for _, sub := range subs {
		err := sub.Handle(ctx, msg)
		if err != nil {
			// TODO faulty implementation, catch later
			return false, err
		}
	}
	return true, nil
}

func wrapSubscriber(subscriber interface{}) (Handler, error) {
	switch h := subscriber.(type) {
	case Handler:
		return h, nil
	default:
		return nil, fmt.Errorf("no way to handle")
	}
}
