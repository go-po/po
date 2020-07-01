package rabbitmq

import (
	"context"
	"strings"

	"github.com/go-po/po/internal/broker"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/streams"
	"github.com/streadway/amqp"
)

type Logger interface {
	Errorf(template string, args ...interface{})
}

func NewTransport(cfg Config, log Logger) *Transport {
	return &Transport{
		cfg: cfg,
		log: log,
	}
}

type Transport struct {
	cfg Config
	log Logger
}

func (t *Transport) Register(ctx context.Context, group string, consumer broker.RecordHandler) (broker.RecordHandler, error) {
	err := t.newConsume(ctx, group, consumer)
	if err != nil {
		return nil, err
	}
	publisher, err := newPublish(t.cfg, group)
	if err != nil {
		return nil, err
	}

	return publisher, nil
}

func (t *Transport) newConsume(ctx context.Context, group string, input broker.RecordHandler) error {
	ch, err := connect(t.cfg)
	if err != nil {
		return err
	}

	queue, err := ch.QueueDeclare(
		"po.stream."+group, // name of the queue
		true,               // durable
		false,              // delete when unused
		false,              // exclusive
		false,              // noWait
		nil,                // arguments

	)
	if err != nil {
		return err
	}

	err = ch.QueueBind(
		queue.Name, // name of the queue
		routingKey(t.cfg.Exchange, "stream", group), // bindingKey
		t.cfg.Exchange, // sourceExchange
		false,          // noWait
		nil,            // arguments
	)
	if err != nil {
		return err
	}

	deliveries, err := ch.Consume(
		queue.Name, // name
		t.cfg.Id,   // consumerTag, unique id for the consumer on the given queue
		false,      // noAck, false means deliveries should call Ack/NoAck explicitly
		false,      // exclusive, false to allow others to consume the same queue
		false,      // noLocal
		false,      // noWait
		nil,        // arguments
	)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				break
			case msg := <-deliveries:
				t.consumeMessage(msg, input)
			}
		}
	}()

	return nil
}

func (t *Transport) consumeMessage(msg amqp.Delivery, input broker.RecordHandler) {
	streamId, number, globalNumber, err := broker.ParseMessageId(msg.MessageId)
	if err != nil {
		t.log.Errorf("parse message id: %s", err)
		err = msg.Nack(false, false)
		if err != nil {
			t.log.Errorf("nack message: %s", err)
		}
		return
	}
	stream := streams.ParseId(streamId)
	ack, err := input.Handle(context.Background(), record.Record{
		Number:        number,
		Stream:        stream,
		Data:          msg.Body,
		Group:         stream.Group,
		ContentType:   msg.ContentType,
		GroupNumber:   globalNumber,
		Time:          msg.Timestamp,
		CorrelationId: msg.CorrelationId,
	})
	if err != nil {
		t.log.Errorf("handle message: %s", err)
		err = msg.Nack(false, true)
		if err != nil {
			t.log.Errorf("nack message: %s", err)
		}
		return
	}
	if ack {
		err = msg.Ack(true)
		if err != nil {
			t.log.Errorf("ack message: %s", err)
		}
	}
}

func newPublish(cfg Config, group string) (broker.RecordHandlerFunc, error) {
	ch, err := connect(cfg)
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context, r record.Record) (bool, error) {
		err := ch.Publish(cfg.Exchange,
			routingKey(cfg.Exchange, "stream", group), // routing key,
			false, // mandatory
			false, // immediate
			amqp.Publishing{
				Headers:         amqp.Table{},
				ContentType:     r.ContentType,
				ContentEncoding: "",
				DeliveryMode:    amqp.Transient,
				Priority:        0,
				CorrelationId:   r.CorrelationId,
				Expiration:      "",
				MessageId:       broker.ToMessageId(r),
				Timestamp:       r.Time,
				Type:            r.Group,
				UserId:          "",
				AppId:           "",
				Body:            r.Data,
			})
		return true, err
	}, nil
}

func connect(cfg Config) (*amqp.Channel, error) {
	conn, err := amqp.Dial(cfg.AmqpUrl)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	return channel, channel.ExchangeDeclare(
		cfg.Exchange, // name
		"direct",     // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // noWait
		nil,          // arguments
	)
}

func routingKey(exchange, flow, group string) string {
	return strings.Join([]string{exchange, flow, group}, ".")
}
