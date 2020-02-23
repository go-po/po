package rabbitmq

import (
	"context"
	"fmt"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/stream"
	"github.com/streadway/amqp"
	"sync"
)

func newSubscriber(broker *Broker) *Subscriber {
	return &Subscriber{
		broker: broker,
		stream: make(map[string]amqp.Queue),
		assign: make(map[string]amqp.Queue),
	}
}

type Subscriber struct {
	broker  *Broker
	channel *amqp.Channel
	mu      sync.Mutex // protects the stream map
	stream  map[string]amqp.Queue
	assign  map[string]amqp.Queue
}

func (sub *Subscriber) connect() error {
	var err error
	sub.channel, err = sub.broker.connect()
	return err
}
func (sub *Subscriber) subscribe(ctx context.Context, id stream.Id) error {
	err := sub.subscribeAssign(ctx, id)
	if err != nil {
		return err
	}
	err = sub.subscribeStream(ctx, id)
	if err != nil {
		return err
	}
	return nil
}

func (sub *Subscriber) subscribeAssign(ctx context.Context, id stream.Id) error {
	sub.mu.Lock()
	defer sub.mu.Unlock()

	queue, hasQueue := sub.stream[id.Group]
	if hasQueue {
		// already defined, bail out now
		return nil
	}

	queue, err := sub.channel.QueueDeclare(
		"po.assign."+id.Group, // name of the queue
		true,                  // durable
		false,                 // delete when unused
		false,                 // exclusive
		false,                 // noWait
		amqp.Table{ // arguments
			"x-single-active-consumer": true,
		}, // arguments

	)
	if err != nil {
		return err
	}

	err = sub.channel.QueueBind(
		queue.Name, // name of the queue
		routingKey(sub.broker.ConnInfo.Exchange, "assign", id.Group), // bindingKey
		sub.broker.ConnInfo.Exchange,                                 // sourceExchange
		false,                                                        // noWait
		nil,                                                          // arguments
	)
	if err != nil {
		return err
	}

	deliveries, err := sub.channel.Consume(
		queue.Name, // name
		"",         // consumerTag, unique id for the consumer on the given queue
		false,      // noAck, false means deliveries should call Ack/NoAck explicitly
		false,      // exclusive, false to allow others to consume the same queue
		false,      // noLocal
		false,      // noWait
		nil,        // arguments
	)

	// closes when the deliverers are closed, which happens along with the amqp channel
	go sub.deliverAssign(deliveries)

	sub.assign[id.Group] = queue

	return nil
}

func (sub *Subscriber) subscribeStream(ctx context.Context, id stream.Id) error {
	sub.mu.Lock()
	defer sub.mu.Unlock()

	queue, hasQueue := sub.stream[id.Group]
	if hasQueue {
		// already defined, bail out now
		return nil
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
		return err
	}

	err = sub.channel.QueueBind(
		queue.Name, // name of the queue
		routingKey(sub.broker.ConnInfo.Exchange, "stream", id.Group), // bindingKey
		sub.broker.ConnInfo.Exchange,                                 // sourceExchange
		false,                                                        // noWait
		nil,                                                          // arguments
	)
	if err != nil {
		return err
	}

	deliveries, err := sub.channel.Consume(
		queue.Name, // name
		id.Group,   // consumerTag, unique id for the consumer on the given queue
		false,      // noAck, false means deliveries should call Ack/NoAck explicitly
		false,      // exclusive, false to allow others to consume the same queue
		false,      // noLocal
		false,      // noWait
		nil,        // arguments
	)

	// closes when the deliverers are closed, which happens along with the amqp channel
	go sub.deliverStream(deliveries)

	sub.stream[id.Group] = queue

	return nil
}

func (sub *Subscriber) deliverAssign(deliveries <-chan amqp.Delivery) {
	count := 0
	for msg := range deliveries {
		streamId, number, _, err := parseMessageId(msg.MessageId)
		if err != nil {
			// TODO
			fmt.Printf("assign parse id: %s\n", err)
		}
		count = count + 1
		r, err := sub.broker.assigner.AssignGroup(context.Background(), stream.ParseId(streamId), number)
		if err != nil {
			// TODO
			fmt.Printf("assign group [%s:%d]: %s\n", streamId, number, err)
		}

		err = sub.broker.pub.notify(context.Background(), r)
		if err != nil {
			// TODO
			fmt.Printf("assign notify: %s\n", err)
		}

		// TODO figure out how to use Nack in this case
		err = msg.Ack(false)
		if err != nil {
			// TODO
			fmt.Printf("assign ack: %s\n", err)
		}
	}
}
func (sub *Subscriber) deliverStream(deliveries <-chan amqp.Delivery) {
	for msg := range deliveries {
		streamId, number, groupNumber, err := parseMessageId(msg.MessageId)
		id := stream.ParseId(streamId)
		rec := record.Record{
			Number:      number,
			Stream:      id,
			Data:        msg.Body,
			Group:       id.Group,
			ContentType: msg.ContentType,
			GroupNumber: groupNumber,
			Time:        msg.Timestamp,
		}

		_, err = sub.broker.distributor.Distribute(context.Background(), rec)
		if err != nil {
			// TODO
			fmt.Printf("stream distribute: %s\n", err)
		}

		// TODO figure out how to use Nack in this case
		err = msg.Ack(true)
		if err != nil {
			// TODO
			fmt.Printf("stream acking: %s\n", err)
		}
	}
}
