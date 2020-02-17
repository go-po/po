package distributor

import (
	"context"
	"fmt"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/stream"
	"sync"
)

func New(groupNumbers groupNumberAssigner, registry stream.Registry) *distributor {
	return &distributor{
		subs:         make(map[string][]stream.Handler),
		groupNumbers: groupNumbers,
		registry:     registry,
	}
}

type groupNumberAssigner interface {
	AssignGroupNumber(ctx context.Context, r record.Record) (int64, error)
}

type distributor struct {
	groupNumbers groupNumberAssigner
	registry     stream.Registry
	mu           sync.Mutex                  // guards below maps
	subs         map[string][]stream.Handler // stream group to handler
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
	id := stream.ParseId(record.Stream)
	subs, hasSubs := dist.subs[id.Group]
	if !hasSubs {
		return false, nil
	}
	var msg stream.Message
	msg, err := stream.ToMessage(dist.registry, record)
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

func wrapSubscriber(subscriber interface{}) (stream.Handler, error) {
	switch h := subscriber.(type) {
	case stream.Handler:
		return h, nil
	default:
		return nil, fmt.Errorf("no way to handle")
	}
}
