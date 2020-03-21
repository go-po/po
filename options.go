package po

import (
	"database/sql"
	"fmt"
	"github.com/go-po/po/internal/broker"
	"github.com/go-po/po/internal/broker/channels"
	"github.com/go-po/po/internal/broker/rabbitmq"
	"github.com/go-po/po/internal/distributor"
	"github.com/go-po/po/internal/registry"
	"github.com/go-po/po/internal/store/inmemory"
	"github.com/go-po/po/internal/store/postgres"
)

type Options struct {
	store    Store
	protocol broker.Protocol
}

type Option func(opt *Options) error

func WithStoreInMemory() Option {
	return func(opt *Options) error {
		opt.store = inmemory.New()
		return nil
	}
}

func WithStorePGUrl(connectionUrl string) Option {
	return func(opt *Options) (err error) {
		opt.store, err = postgres.NewFromUrl(connectionUrl)
		return
	}
}

func WithStorePGConn(db *sql.DB) Option {
	return func(opt *Options) (err error) {
		opt.store, err = postgres.New(db)
		return
	}
}

func WithProtocolChannels() Option {
	return func(opt *Options) error {
		opt.protocol = channels.New()
		return nil
	}
}

func WithProtocolRabbitMQ(url, exchange, id string) Option {
	return func(opt *Options) (err error) {
		opt.protocol = rabbitmq.New(rabbitmq.Config{
			AmqpUrl:  url,
			Exchange: exchange,
			Id:       id,
		})
		return
	}
}

func New(store Store, protocol broker.Protocol) *Po {
	dist := distributor.New(registry.DefaultRegistry, store)
	broker := broker.New(protocol, dist, store)
	return &Po{
		store:  store,
		broker: broker,
	}
}

func NewFromOptions(opts ...Option) (*Po, error) {
	options := &Options{}

	for _, opt := range opts {
		err := opt(options)
		if err != nil {
			return nil, err
		}
	}

	if options.store == nil {
		return nil, fmt.Errorf("po: no store provided")
	}
	if options.protocol == nil {
		return nil, fmt.Errorf("po: no protocol provided")
	}

	return New(options.store, options.protocol), nil
}
