package rabbitmq

import (
	"context"
	"fmt"
	"github.com/go-po/po"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/registry"
	"github.com/streadway/amqp"
)

func newSubscriber(broker *Broker) *Subscriber {
	return &Subscriber{
		broker: broker,
		queues: make(map[string]amqp.Queue),
	}
}

type Subscriber struct {
	broker  *Broker
	channel *amqp.Channel
	queues  map[string]amqp.Queue
}

func (sub *Subscriber) connect() error {
	var err error
	sub.channel, err = sub.broker.connect()
	return err
}

func (sub *Subscriber) subscribe(ctx context.Context, subscriptionId string, id po.StreamId, subscriber interface{}) error {
	queue, err := sub.getQueue(id)
	if err != nil {
		return err
	}

	deliveries, err := sub.channel.Consume(
		queue.Name,     // name
		subscriptionId, // consumerTag,
		false,          // noAck
		false,          // exclusive
		false,          // noLocal
		false,          // noWait
		nil,            // arguments
	)
	handler, err := wrapSubscriber(subscriber)
	if err != nil {
		return err
	}
	go func() {
		for msg := range deliveries {
			stream, number, groupNumber, err := parseMessageId(msg.MessageId)
			r := record.Record{
				Number:      number,
				Stream:      stream,
				Data:        msg.Body,
				Type:        msg.Type,
				GroupNumber: groupNumber,
				Time:        msg.Timestamp,
			}

			message, err := po.ToMessage(registry.DefaultRegistry, r)
			if err != nil {
				// TODO
				fmt.Printf("Failed parsing: %s", err)
			}

			err = handler.Handle(context.Background(), message)
			if err != nil {
				// TODO
				fmt.Printf("failed handling: %s", err)
			}

			err = msg.Ack(true)
			if err != nil {
				// TODO
				fmt.Printf("failed acking: %s", err)
			}
		}
	}()

	return nil
}

func (sub *Subscriber) getQueue(id po.StreamId) (amqp.Queue, error) {
	queue, hasQueue := sub.queues[id.Group]
	if hasQueue {
		return queue, nil
	}
	queue, err := sub.channel.QueueDeclare(
		"po.stream."+id.Group, // name of the queue
		true,                  // durable
		false,                 // delete when unused
		false,                 // exclusive
		false,                 // noWait
		nil,                   // arguments

	)
	if err != nil {
		return amqp.Queue{}, err
	}
	sub.queues[id.Group] = queue

	err = sub.channel.QueueBind(
		queue.Name,                   // name of the queue
		id.Group,                     // bindingKey
		sub.broker.ConnInfo.Exchange, // sourceExchange
		false,                        // noWait
		nil,                          // arguments
	)
	if err != nil {
		return amqp.Queue{}, err
	}

	return queue, nil
}

func wrapSubscriber(subscriber interface{}) (po.Handler, error) {
	switch h := subscriber.(type) {
	case po.Handler:
		return h, nil
	default:
		return nil, fmt.Errorf("no way to handle")
	}
}
