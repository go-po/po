package broker

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/streams"
)

type RecordAck func() (record.Record, func() error)
type MessageIdAck func() (string, func() error)

type ProtocolPipes interface {
	AssignNotify() chan<- string // message id
	Assign() <-chan MessageIdAck
	StreamNotify() chan<- record.Record
	Stream() <-chan RecordAck
	Err() <-chan error
	Ctx() context.Context
}

// abstract the used distribution
// protocol away from the Broker logic
type Protocol interface {
	// // Start listening for channels relevant to the id
	Register(ctx context.Context, id streams.Id) (ProtocolPipes, error)
}

func NewGroupNumberBroker(protocol Protocol, distributor Distributor, groupAssigner GroupAssigner, observer Observer) *GroupNumberBroker {
	return &GroupNumberBroker{
		protocol:      protocol,
		distributor:   distributor,
		groupAssigner: groupAssigner,
		mu:            sync.RWMutex{},
		pipes:         make(map[string]ProtocolPipes),
		observer:      observer,
	}
}

type GroupNumberBroker struct {
	protocol      Protocol
	distributor   Distributor
	groupAssigner GroupAssigner
	mu            sync.RWMutex             // protects the pipes
	pipes         map[string]ProtocolPipes // group id to the pipes
	observer      Observer
}

func (broker *GroupNumberBroker) Notify(ctx context.Context, records ...record.Record) error {
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

func (broker *GroupNumberBroker) notifyRecord(ctx context.Context, r record.Record) error {
	done := broker.observer.AssignNotify(ctx, r.Stream)
	defer done()

	pipe, found := broker.pipes[r.Group]
	if !found {
		return fmt.Errorf("internal/broker group not subscribed: %s", r.Group)
	}
	pipe.AssignNotify() <- ToMessageId(r)
	return nil
}

func (broker *GroupNumberBroker) Register(ctx context.Context, subscriberId string, streamId streams.Id, subscriber interface{}) error {

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

	go broker.flowAssign(pipe)
	go broker.flowStream(pipe)

	return nil
}

func (broker *GroupNumberBroker) flowAssign(pipe ProtocolPipes) {
	for {
		select {
		case <-pipe.Ctx().Done():
			break
		case msgIdAck := <-pipe.Assign():
			broker.onAssignMessage(pipe, msgIdAck)
		}
	}
}

func (broker *GroupNumberBroker) onAssignMessage(pipe ProtocolPipes, msgIdAck MessageIdAck) {
	// TODO Add Start Span here
	ctx := context.Background()
	msgId, ack := msgIdAck()
	streamId, number, _, err := ParseMessageId(msgId)
	if err != nil {
		done := broker.observer.AssignErrorParseId(ctx, err)
		defer done()
	}

	id := streams.ParseId(streamId)

	done := broker.observer.AssignNotifyReceived(ctx, id)
	defer done()

	r, err := broker.groupAssigner.AssignGroup(ctx, id, number)
	if err != nil {
		done := broker.observer.AssignErrorGroupNumber(ctx, id, err)
		defer done()
		return
	}

	streamDone := broker.observer.StreamNotify(ctx, id)
	defer streamDone()
	pipe.StreamNotify() <- r

	_ = ack()
}

func (broker *GroupNumberBroker) flowStream(pipe ProtocolPipes) {
	for {
		select {
		case <-pipe.Ctx().Done():
			break
		case recordAck := <-pipe.Stream():
			broker.onStreamMessage(recordAck)
		}
	}
}

func (broker *GroupNumberBroker) onStreamMessage(recordAck RecordAck) {
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
