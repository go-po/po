package rabbitmq

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-po/po/internal/broker"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/streams"
	"github.com/streadway/amqp"
)

func New(amqpUrl string, exchange string, instanceId string, opts ...Option) *Transport {
	var defaultConfig = config{
		Log:             noopLogger{},
		AmqpUrl:         amqpUrl,
		Exchange:        exchange,
		Id:              instanceId,
		QueueNamePrefix: "",
		MiddlewarePublish: func(ctx context.Context, msg amqp.Publishing, next func(context.Context, amqp.Publishing) error) error {
			return next(ctx, msg)
		},
		MiddlewareConsume: func(ctx context.Context, msg amqp.Delivery, next func(context.Context, amqp.Delivery) error) error {
			return next(ctx, msg)
		},
		ReconnectDelay: time.Second,
	}

	for _, opt := range opts {
		opt(&defaultConfig)
	}

	return &Transport{
		cfg: defaultConfig,
	}
}

type Transport struct {
	cfg config
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
		t.cfg.QueueNamePrefix+"po."+group, // name of the queue
		true,                              // durable
		false,                             // delete when unused
		false,                             // exclusive
		false,                             // noWait
		nil,                               // arguments

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
				t.consumeMessage(context.Background(), msg, input)
			}
		}
	}()

	return nil
}

func (t *Transport) consumeMessage(ctx context.Context, delivery amqp.Delivery, input broker.RecordHandler) {
	var wg sync.WaitGroup
	wg.Add(1)

	err := t.cfg.MiddlewareConsume(ctx, delivery, func(ctx context.Context, msg amqp.Delivery) error {
		defer wg.Done()
		return t.deliverMessage(ctx, delivery, input)
	})

	wg.Wait()

	if err != nil {
		t.cfg.Log.Errorf("po/rabbit handle message: %s", err)
		err = delivery.Nack(false, true)
		if err != nil {
			t.cfg.Log.Errorf("po/rabbit nack message: %s", err)
		}
		return
	}
}

func (t *Transport) deliverMessage(ctx context.Context, msg amqp.Delivery, input broker.RecordHandler) error {

	streamId, number, globalNumber, err := broker.ParseMessageId(msg.MessageId)
	if err != nil {
		err = msg.Nack(false, false)
		if err != nil {
			t.cfg.Log.Errorf("nack message: %s", err)
		}
		return fmt.Errorf("po/rabbit parse message id: %w", err)
	}
	stream := streams.ParseId(streamId)

	ack, err := input.Handle(ctx, record.Record{
		Number:        number,
		Stream:        stream,
		Data:          msg.Body,
		Group:         stream.Group,
		ContentType:   msg.ContentType,
		GlobalNumber:  globalNumber,
		Time:          msg.Timestamp,
		CorrelationId: msg.CorrelationId,
	})
	if err != nil {
		return err
	}

	if ack {
		err = msg.Ack(true)
		if err != nil {
			return fmt.Errorf("po/rabbit failed ack: %w", err)
		}
	}
	return nil
}

func newPublish(cfg config, group string) (broker.RecordHandlerFunc, error) {
	ch, err := connect(cfg)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, r record.Record) (bool, error) {
		var wg sync.WaitGroup
		wg.Add(1)
		err := cfg.MiddlewarePublish(ctx, amqp.Publishing{
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
		}, func(ctx context.Context, msg amqp.Publishing) error {
			defer wg.Done()
			return ch.Publish(cfg.Exchange,
				routingKey(cfg.Exchange, "stream", group), // routing key,
				false, // mandatory
				false, // immediate
				msg)
		})
		wg.Wait()
		return true, err
	}, nil
}

func connect(cfg config) (*Channel, error) {
	conn, err := dial(cfg)
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
