package channels

import (
	"context"
	"github.com/go-po/po/internal/broker"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/stream"
)

func New() *Channels {
	return &Channels{}
}

type Channels struct {
}

func (ch *Channels) Register(ctx context.Context, id stream.Id) (broker.ProtocolPipes, error) {
	pipe := &ChannelPipes{
		ctx:          context.Background(),
		assignNotify: make(chan string),
		assign:       make(chan broker.MessageIdAck),
		streamNotify: make(chan record.Record),
		stream:       make(chan broker.RecordAck),
		errs:         make(chan error),
	}
	go func() {
		for {
			select {
			case <-pipe.ctx.Done():
				break
			case msgId := <-pipe.assignNotify:
				pipe.assign <- func() (string, func() error) {
					return msgId, func() error { return nil }
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-pipe.ctx.Done():
				break
			case r := <-pipe.streamNotify:
				pipe.stream <- func() (record.Record, func() error) {
					return r, func() error { return nil }
				}
			}
		}
	}()
	return pipe, nil
}

type ChannelPipes struct {
	ctx          context.Context
	assignNotify chan string
	assign       chan broker.MessageIdAck
	streamNotify chan record.Record
	stream       chan broker.RecordAck
	errs         chan error
}

func (pipe *ChannelPipes) AssignNotify() chan<- string {
	return pipe.assignNotify
}

func (pipe *ChannelPipes) Assign() <-chan broker.MessageIdAck {
	return pipe.assign
}

func (pipe *ChannelPipes) StreamNotify() chan<- record.Record {
	return pipe.streamNotify
}

func (pipe *ChannelPipes) Stream() <-chan broker.RecordAck {
	return pipe.stream
}

func (pipe *ChannelPipes) Err() <-chan error {
	return pipe.errs
}

func (pipe *ChannelPipes) Ctx() context.Context {
	return pipe.ctx
}

var _ broker.ProtocolPipes = &ChannelPipes{}
