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

func New(cfg Config) *Protocol {
	return &Protocol{cfg: cfg}
}

type Protocol struct {
	cfg Config
}

func (proto *Protocol) Register(ctx context.Context, id stream.Id) (broker.ProtocolPipes, error) {
	return newPipe(proto.cfg, id)
}

var _ broker.Protocol = &Protocol{}

type amqpChan struct {
	channel    *amqp.Channel
	deliveries <-chan amqp.Delivery
}

func parseMessageIdAck(msg amqp.Delivery) broker.MessageIdAck {
	return func() (string, func() error) {
		return msg.MessageId, func() error {
			return msg.Ack(false)
		}
	}
}

func parseRecordAck(msg amqp.Delivery) broker.RecordAck {
	return func() (record.Record, func() error) {
		streamId, number, groupNumber, _ := broker.ParseMessageId(msg.MessageId)
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
		return rec, func() error {
			return msg.Ack(true)
		}
	}
}
