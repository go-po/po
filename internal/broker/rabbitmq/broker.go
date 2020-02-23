package rabbitmq

import (
	"context"
	"github.com/go-po/po"
	"github.com/go-po/po/internal/broker"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/stream"
	"github.com/streadway/amqp"
)

type ConnInfo struct {
	AmqpUrl  string
	Exchange string
}

func New(uri, exchange string, assigner broker.GroupAssigner) (*Broker, error) {
	broker := &Broker{ConnInfo: ConnInfo{
		AmqpUrl:  uri,
		Exchange: exchange,
	},
		assigner: assigner,
	}

	broker.pub = newPublisher(broker)
	broker.sub = newSubscriber(broker)

	err := broker.pub.connect()
	if err != nil {
		return nil, err
	}

	err = broker.sub.connect()
	if err != nil {
		return nil, err
	}

	return broker, nil
}

var _ po.Broker = &Broker{}

type Broker struct {
	ConnInfo    ConnInfo
	pub         *Publisher
	sub         *Subscriber
	distributor broker.Distributor
	assigner    broker.GroupAssigner
}

func (broker *Broker) Distributor(distributor broker.Distributor) {
	broker.distributor = distributor
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
	conn, err := amqp.Dial(broker.ConnInfo.AmqpUrl)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	return channel, channel.ExchangeDeclare(
		broker.ConnInfo.Exchange, // name
		"direct",                 // type
		true,                     // durable
		false,                    // auto-deleted
		false,                    // internal
		false,                    // noWait
		nil,                      // arguments
	)
}

func (broker *Broker) Shutdown() error {
	err := broker.sub.channel.Close()
	if err != nil {
		return err
	}
	err = broker.pub.channel.Close()
	if err != nil {
		return err
	}
	return nil
}
