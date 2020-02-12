package rabbitmq

import (
	"context"
	"github.com/go-po/po"
	"github.com/go-po/po/internal/record"
	"github.com/streadway/amqp"
)

type ConnInfo struct {
	AmqpUrl  string
	Exchange string
}

func New(uri, exchange string) (*Broker, error) {
	broker := &Broker{ConnInfo: ConnInfo{
		AmqpUrl:  uri,
		Exchange: exchange,
	}}

	broker.pub = &Publisher{
		broker: broker,
	}

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
	ConnInfo ConnInfo
	pub      *Publisher
	sub      *Subscriber
}

func (broker *Broker) Notify(ctx context.Context, records ...record.Record) error {
	for _, record := range records {
		err := broker.pub.notify(ctx, record)
		if err != nil {
			return err
		}
	}
	return nil
}

func (broker *Broker) Subscribe(ctx context.Context, subscriptionId, streamId string, subscriber interface{}) error {
	return broker.sub.subscribe(ctx, subscriptionId, po.ParseStreamId(streamId), subscriber)
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
	return nil
}
