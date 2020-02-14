package rabbitmq

import (
	"context"
	"fmt"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/stream"
	"github.com/streadway/amqp"
	"sync"
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
	mu      sync.Mutex // protects the queues map
	queues  map[string]amqp.Queue
}

func (sub *Subscriber) connect() error {
	var err error
	sub.channel, err = sub.broker.connect()
	return err
}

func (sub *Subscriber) subscribe(ctx context.Context, id stream.Id) error {
	sub.mu.Lock()
	defer sub.mu.Unlock()

	queue, hasQueue := sub.queues[id.Group]
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
		queue.Name,                   // name of the queue
		id.Group,                     // bindingKey
		sub.broker.ConnInfo.Exchange, // sourceExchange
		false,                        // noWait
		nil,                          // arguments
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
	go sub.deliver(deliveries)

	sub.queues[id.Group] = queue

	return nil
}

func (sub *Subscriber) deliver(deliveries <-chan amqp.Delivery) {
	for msg := range deliveries {
		stream, number, groupNumber, err := parseMessageId(msg.MessageId)
		rec := record.Record{
			Number:      number,
			Stream:      stream,
			Data:        msg.Body,
			Type:        msg.Type,
			GroupNumber: groupNumber,
			Time:        msg.Timestamp,
		}

		_, err = sub.broker.distributor.Distribute(context.Background(), rec)
		if err != nil {
			// TODO
			fmt.Printf("failed handling: %s", err)
		}

		// TODO figure out how to use Nack in this case
		err = msg.Ack(true)
		if err != nil {
			// TODO
			fmt.Printf("failed acking: %s", err)
		}
	}
}
