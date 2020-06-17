package broker

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/streams"
)

func NewGlobalNumberBroker(protocol Protocol, distributor Distributor, observer GlobalNumberObserver) *GlobalNumberBroker {
	return &GlobalNumberBroker{
		protocol:    protocol,
		distributor: distributor,
		observer:    observer,
		pipes:       make(map[string]ProtocolPipes),
	}
}

type GlobalNumberBroker struct {
	protocol    Protocol
	distributor Distributor

	mu       sync.RWMutex             // protects the pipes
	pipes    map[string]ProtocolPipes // group id to the pipes
	observer GlobalNumberObserver
}

type GlobalNumberObserver interface {
	StreamNotifyReceived(ctx context.Context, stream streams.Id) func()
	StreamErrorDistribute(ctx context.Context, id streams.Id, err error) func()
	StreamNotify(ctx context.Context, id streams.Id) func()
}

func (broker *GlobalNumberBroker) Notify(ctx context.Context, records ...record.Record) error {
	broker.mu.RLock()
	defer broker.mu.RUnlock()
	for _, r := range records {
		err := broker.notifyRecord(ctx, r)
		if err != nil {
			return err
		}
	}
	return nil
}

func (broker *GlobalNumberBroker) Register(ctx context.Context, subscriberId string, streamId streams.Id, subscriber interface{}) error {

	err := broker.distributor.Register(ctx, subscriberId, streamId, subscriber)
	if err != nil {
		return err
	}

	broker.mu.Lock()
	defer broker.mu.Unlock()

	if broker.pipes[streamId.Group] != nil {
		// already registered
		return nil
	}

	pipe, err := broker.protocol.Register(ctx, streamId)
	if err != nil {
		return err
	}
	broker.pipes[streamId.Group] = pipe

	go broker.flowStream(pipe)

	return nil
}

func (broker *GlobalNumberBroker) notifyRecord(ctx context.Context, r record.Record) error {
	done := broker.observer.StreamNotify(ctx, r.Stream)
	defer done()

	pipe, found := broker.pipes[r.Group]
	if !found {
		return fmt.Errorf("internal/broker group not subscribed: %s", r.Group)
	}
	pipe.StreamNotify() <- r
	return nil
}

func (broker *GlobalNumberBroker) flowStream(pipe ProtocolPipes) {
	for {
		select {
		case <-pipe.Ctx().Done():
			break
		case recordAck := <-pipe.Stream():
			broker.onStreamMessage(recordAck)
		}
	}
}

func (broker *GlobalNumberBroker) onStreamMessage(recordAck RecordAck) {
	// TODO Add Start Span here
	ctx := context.Background()

	r, ack := recordAck()
	done := broker.observer.StreamNotifyReceived(ctx, r.Stream)
	defer done()

	_, err := broker.distributor.Distribute(ctx, r)
	if err != nil {
		broker.observer.StreamErrorDistribute(ctx, r.Stream, err)
	}
	_ = ack()
}
