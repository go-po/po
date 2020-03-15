package broker

import (
	"context"
	"fmt"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/stream"
	"sync"
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
	Register(ctx context.Context, id stream.Id) (ProtocolPipes, error)
}

func New(protocol Protocol, distributor Distributor, groupAssigner GroupAssigner) *Broker {
	return &Broker{
		protocol:      protocol,
		distributor:   distributor,
		groupAssigner: groupAssigner,
		mu:            sync.RWMutex{},
		pipes:         make(map[string]ProtocolPipes),
	}
}

type Broker struct {
	protocol      Protocol
	distributor   Distributor
	groupAssigner GroupAssigner
	mu            sync.RWMutex             // protects the pipes
	pipes         map[string]ProtocolPipes // group id to the pipes
}

func (broker *Broker) Notify(ctx context.Context, records ...record.Record) error {
	broker.mu.RLock()
	defer broker.mu.RUnlock()
	for _, r := range records {
		pipe, found := broker.pipes[r.Group]
		if !found {
			return fmt.Errorf("internal/broker group not subscribed: %s", r.Group)
		}
		pipe.AssignNotify() <- ToMessageId(r)
	}
	return nil
}

func (broker *Broker) Subscribe(ctx context.Context, streamId stream.Id) error {
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

func (broker *Broker) flowAssign(pipe ProtocolPipes) {
	for {
		select {
		case <-pipe.Ctx().Done():
			break
		case msgIdAck := <-pipe.Assign():
			msgId, ack := msgIdAck()
			streamId, number, _, err := ParseMessageId(msgId)
			if err != nil {
				// TODO
				fmt.Printf("assign parse id: %s\n", err)
			}

			r, err := broker.groupAssigner.AssignGroup(context.Background(), stream.ParseId(streamId), number)
			if err != nil {
				// TODO
				fmt.Printf("assign group [%s:%d]: %s\n", streamId, number, err)
			}

			pipe.StreamNotify() <- r

			_ = ack()
		}
	}
}

func (broker *Broker) flowStream(pipe ProtocolPipes) {
	for {
		select {
		case <-pipe.Ctx().Done():
			break
		case recordAck := <-pipe.Stream():
			r, ack := recordAck()
			_, err := broker.distributor.Distribute(context.Background(), r)
			if err != nil {
				// TODO
				fmt.Printf("stream distribute: %s\n", err)
			}
			_ = ack()
		}
	}
}
