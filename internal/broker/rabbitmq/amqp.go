package rabbitmq

import (
	"github.com/go-po/po/stream"
	"github.com/streadway/amqp"
	"strings"
)

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

func newAssignChannel(cfg Config, id stream.Id) (*amqpChan, error) {
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
		routingKey(cfg.Exchange, "assignAmqp", id.Group), // bindingKey
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
	return &amqpChan{
		channel:    channel,
		deliveries: deliveries,
	}, nil
}

func newStreamChannel(cfg Config, id stream.Id) (*amqpChan, error) {

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
		routingKey(cfg.Exchange, "streamAmqp", id.Group), // bindingKey
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
