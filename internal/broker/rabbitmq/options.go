package rabbitmq

import (
	"context"

	"github.com/streadway/amqp"
)

type MiddlewarePublish func(ctx context.Context, msg amqp.Publishing, next func(context.Context, amqp.Publishing) error) error
type MiddlewareConsume func(ctx context.Context, msg amqp.Delivery, next func(context.Context, amqp.Delivery) error) error

type config struct {
	Log               Logger
	AmqpUrl           string
	Exchange          string
	Id                string
	QueueNamePrefix   string
	MiddlewarePublish MiddlewarePublish
	MiddlewareConsume MiddlewareConsume
}

type Option func(opt *config)

func WithQueueNamePrefix(prefix string) Option {
	return func(opt *config) {
		opt.QueueNamePrefix = prefix
	}
}

func WithPublisherMiddleware(middleware MiddlewarePublish) Option {
	return func(opt *config) {
		opt.MiddlewarePublish = middleware
	}
}

func WithConsumerMiddleware(middleware MiddlewareConsume) Option {
	return func(opt *config) {
		opt.MiddlewareConsume = middleware
	}
}

func WithLogger(log Logger) Option {
	return func(opt *config) {
		opt.Log = log
	}
}
