package rabbitmq

import (
	"context"
	"github.com/go-po/po/internal/record"
	"github.com/streadway/amqp"
	"strings"
)

func newPublisher(broker *Broker) *Publisher {
	return &Publisher{
		broker: broker,
	}
}

type Publisher struct {
	broker  *Broker
	channel *amqp.Channel
}

func routingKey(exchange, flow, group string) string {
	return strings.Join([]string{exchange, flow, group}, ".")
}

// first part of the step, puts the record on an assign queue
// here it will be read sequential by a single consumer in
// order to assign a group number
// Afterwards the record will be republished on the stream queue
// and distributed to all listeners.
func (pub *Publisher) assign(ctx context.Context, record record.Record) error {
	return pub.channel.Publish(pub.broker.Config.Exchange,
		routingKey(pub.broker.Config.Exchange, "assign", record.Stream.Group), // routing key,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			DeliveryMode:    amqp.Transient,
			Priority:        0,
			CorrelationId:   "",
			Expiration:      "",
			MessageId:       toMessageId(record),
			Timestamp:       record.Time,
			Type:            record.Group,
		})
}

func (pub *Publisher) notify(ctx context.Context, record record.Record) error {
	return pub.channel.Publish(pub.broker.Config.Exchange,
		routingKey(pub.broker.Config.Exchange, "stream", record.Stream.Group), // routing key,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     record.ContentType,
			ContentEncoding: "",
			DeliveryMode:    amqp.Transient,
			Priority:        0,
			CorrelationId:   "",
			Expiration:      "",
			MessageId:       toMessageId(record),
			Timestamp:       record.Time,
			Type:            record.Group,
			UserId:          "",
			AppId:           "",
			Body:            record.Data,
		})
}

func (pub *Publisher) connect() error {
	var err error
	pub.channel, err = pub.broker.connect()
	return err
}
