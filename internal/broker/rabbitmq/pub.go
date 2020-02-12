package rabbitmq

import (
	"context"
	"github.com/go-po/po"
	"github.com/go-po/po/internal/record"
	"github.com/streadway/amqp"
)

type Publisher struct {
	broker  *Broker
	channel *amqp.Channel
}

func (pub *Publisher) notify(ctx context.Context, record record.Record) error {
	id := po.ParseStreamId(record.Stream)
	return pub.channel.Publish(pub.broker.ConnInfo.Exchange,
		id.Group, // routing key,
		false,    // mandatory
		false,    // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "application/json",
			ContentEncoding: "",
			DeliveryMode:    amqp.Transient,
			Priority:        0,
			CorrelationId:   "",
			Expiration:      "",
			MessageId:       toMessageId(record),
			Timestamp:       record.Time,
			Type:            record.Type,
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
