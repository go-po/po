package rabbitmq

import (
	"context"
	"github.com/go-po/po/internal/broker"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/stream"
	"github.com/streadway/amqp"
	"time"
)

func newPipe(cfg Config, id stream.Id) (*RabbitPipe, error) {
	assignAmqp, err := newAssignChannel(cfg, id)
	if err != nil {
		return nil, err
	}

	streamAmqp, err := newStreamChannel(cfg, id)
	if err != nil {
		return nil, err
	}

	assignPublish, err := connect(cfg)
	if err != nil {
		return nil, err
	}

	streamPublish, err := connect(cfg)
	if err != nil {
		return nil, err
	}

	pipe := &RabbitPipe{
		ctx: context.Background(),
		err: make(chan error),
		cfg: cfg,

		assignPublishChannel: assignPublish,
		assignNotify:         make(chan string),
		assignAmqp:           assignAmqp,
		assign:               make(chan broker.MessageIdAck),

		streamPublishChannel: streamPublish,
		streamNotify:         make(chan record.Record),
		streamAmqp:           streamAmqp,
		stream:               make(chan broker.RecordAck),
	}

	go func() {
		for {
			select {
			case <-pipe.ctx.Done():
				break
			case msgId := <-pipe.assignNotify:
				err := pipe.publishAssign(msgId)
				if err != nil {
					// TODO consider this
					pipe.err <- err
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-pipe.ctx.Done():
				break
			case msg := <-assignAmqp.deliveries:
				pipe.assign <- parseMessageIdAck(msg)
			}
		}
	}()

	go func() {
		for {
			select {
			case <-pipe.ctx.Done():
				break
			case rec := <-pipe.streamNotify:
				err := pipe.publishStream(rec)
				if err != nil {
					// TODO consider this
					pipe.err <- err
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-pipe.ctx.Done():
				break
			case msg := <-streamAmqp.deliveries:
				pipe.stream <- parseRecordAck(msg)
			}
		}

	}()

	return pipe, nil

}

type RabbitPipe struct {
	ctx context.Context
	err chan error
	cfg Config

	assignPublishChannel *amqp.Channel
	assignNotify         chan string
	assignAmqp           *amqpChan
	assign               chan broker.MessageIdAck

	streamPublishChannel *amqp.Channel
	streamNotify         chan record.Record
	streamAmqp           *amqpChan
	stream               chan broker.RecordAck
}

func (pipe *RabbitPipe) AssignNotify() chan<- string {
	return pipe.assignNotify
}

func (pipe *RabbitPipe) Assign() <-chan broker.MessageIdAck {
	return pipe.assign
}

func (pipe *RabbitPipe) StreamNotify() chan<- record.Record {
	return pipe.streamNotify
}

func (pipe *RabbitPipe) Stream() <-chan broker.RecordAck {
	return pipe.stream
}

func (pipe *RabbitPipe) Err() <-chan error {
	return pipe.err
}

func (pipe *RabbitPipe) Ctx() context.Context {
	return pipe.ctx
}

func (pipe *RabbitPipe) publishAssign(msgId string) error {
	streamId, _, _, err := broker.ParseMessageId(msgId)
	if err != nil {
		return err
	}
	id := stream.ParseId(streamId)
	return pipe.assignPublishChannel.Publish(pipe.cfg.Exchange,
		routingKey(pipe.cfg.Exchange, "assign", id.Group), // routing key,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			DeliveryMode:    amqp.Transient,
			Priority:        0,
			CorrelationId:   "",
			Expiration:      "",
			MessageId:       msgId,
			Timestamp:       time.Now(),
			Type:            id.Group,
		})
}

func (pipe *RabbitPipe) publishStream(r record.Record) error {
	return pipe.streamPublishChannel.Publish(pipe.cfg.Exchange,
		routingKey(pipe.cfg.Exchange, "stream", r.Stream.Group), // routing key,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     r.ContentType,
			ContentEncoding: "",
			DeliveryMode:    amqp.Transient,
			Priority:        0,
			CorrelationId:   "",
			Expiration:      "",
			MessageId:       broker.ToMessageId(r),
			Timestamp:       r.Time,
			Type:            r.Group,
			UserId:          "",
			AppId:           "",
			Body:            r.Data,
		})
}

var _ broker.ProtocolPipes = &RabbitPipe{}
