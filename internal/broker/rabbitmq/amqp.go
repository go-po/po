package rabbitmq

import (
	"github.com/go-po/po/streams"
	"github.com/streadway/amqp"
)

func newAssignChannel(cfg Config, id streams.Id) (*amqpChan, error) {
	channel, err := connect(cfg)
	if err != nil {
		return nil, err
	}

	queue, err := channel.QueueDeclare(
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
		return nil, err
	}

	err = channel.QueueBind(
		queue.Name, // name of the queue
		routingKey(cfg.Exchange, "assign", id.Group), // bindingKey
		cfg.Exchange, // sourceExchange
		false,        // noWait
		nil,          // arguments
	)
	if err != nil {
		return nil, err
	}

	deliveries, err := channel.Consume(
		queue.Name,   // name
		cfg.Exchange, // consumerTag, unique id for the consumer on the given queue
		false,        // noAck, false means deliveries should call Ack/NoAck explicitly
		false,        // exclusive, false to allow others to consume the same queue
		false,        // noLocal
		false,        // noWait
		nil,          // arguments
	)
	if err != nil {
		return nil, err
	}
	return &amqpChan{
		channel:    channel,
		deliveries: deliveries,
	}, nil
}

func newStreamChannel(cfg Config, id streams.Id) (*amqpChan, error) {

	channel, err := connect(cfg)
	if err != nil {
		return nil, err
	}
	queue, err := channel.QueueDeclare(
		"po.stream."+id.Group, // name of the queue
		true,                  // durable
		false,                 // delete when unused
		false,                 // exclusive
		false,                 // noWait
		nil,                   // arguments

	)
	if err != nil {
		return nil, err
	}

	err = channel.QueueBind(
		queue.Name, // name of the queue
		routingKey(cfg.Exchange, "stream", id.Group), // bindingKey
		cfg.Exchange, // sourceExchange
		false,        // noWait
		nil,          // arguments
	)
	if err != nil {
		return nil, err
	}

	deliveries, err := channel.Consume(
		queue.Name, // name
		cfg.Id,     // consumerTag, unique id for the consumer on the given queue
		false,      // noAck, false means deliveries should call Ack/NoAck explicitly
		false,      // exclusive, false to allow others to consume the same queue
		false,      // noLocal
		false,      // noWait
		nil,        // arguments
	)
	if err != nil {
		return nil, err
	}

	return &amqpChan{
		channel:    channel,
		deliveries: deliveries,
	}, nil

}
