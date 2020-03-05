package rabbitmq

import (
	"context"
	"github.com/go-po/po/internal/broker"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/stream"
	"github.com/streadway/amqp"
)

type Config struct {
	AmqpUrl  string
	Exchange string
	Id       string
}

func New(uri, exchange, id string) (*Broker, error) {
	broker := &Broker{Config: Config{
		AmqpUrl:  uri,
		Exchange: exchange,
		Id:       id,
	},
	}

	broker.pub = newPublisher(broker)
	broker.sub = newSubscriber(broker)

	err := broker.pub.connect()
	if err != nil {
		return nil, err
	}

	return broker, nil
}

type Broker struct {
	Config      Config
	pub         *Publisher
	sub         *Subscriber
	distributor broker.Distributor
	assigner    broker.GroupAssigner
}

func (broker *Broker) Prepare(distributor broker.Distributor, groupAssigner broker.GroupAssigner) {
	broker.distributor = distributor
	broker.assigner = groupAssigner
}

func (broker *Broker) Notify(ctx context.Context, records ...record.Record) error {
	for _, record := range records {
		err := broker.pub.assign(ctx, record)
		if err != nil {
			return err
		}
	}
	return nil
}

func (broker *Broker) Subscribe(ctx context.Context, streamId stream.Id) error {
	return broker.sub.subscribe(ctx, streamId)
}

func (broker *Broker) connect() (*amqp.Channel, error) {
	conn, err := amqp.Dial(broker.Config.AmqpUrl)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	return channel, channel.ExchangeDeclare(
		broker.Config.Exchange, // name
		"direct",               // type
		true,                   // durable
		false,                  // auto-deleted
		false,                  // internal
		false,                  // noWait
		nil,                    // arguments
	)
}

func (broker *Broker) Shutdown() error {
	return nil
}
