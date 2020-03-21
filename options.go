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
	registry Registry
}

type Option func(opt *Options) error

func New(store Store, protocol broker.Protocol) *Po {
	return newPo(store, protocol, registry.DefaultRegistry)
}

func NewFromOptions(opts ...Option) (*Po, error) {
	options := &Options{
		registry: registry.DefaultRegistry,
	}

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
	if options.registry == nil {
		return nil, fmt.Errorf("po: no registry provided")
	}

	return newPo(options.store, options.protocol, options.registry), nil
}

func newPo(store Store, protocol broker.Protocol, registry Registry) *Po {
	return &Po{
		store: store,
		broker: broker.New(
			protocol,
			distributor.New(registry, store),
			store,
		),
		registry: registry,
	}
}

// Available options

func WithStoreInMemory() Option {
	return func(opt *Options) error {
		opt.store = NewStoreInMemory()
		return nil
	}
}

func WithRegistry(registry Registry) Option {
	return func(opt *Options) error {
		opt.registry = registry
		return nil
	}
}

func WithStorePostgresUrl(connectionUrl string) Option {
	return func(opt *Options) (err error) {
		opt.store, err = NewStorePostgresUrl(connectionUrl)
		return
	}
}

func WithStorePostgresDB(db *sql.DB) Option {
	return func(opt *Options) (err error) {
		opt.store, err = NewStorePostgresDB(db)
		return
	}
}

func WithProtocolChannels() Option {
	return func(opt *Options) error {
		opt.protocol = NewProtocolChannels()
		return nil
	}
}

func WithProtocolRabbitMQ(url, exchange, id string) Option {
	return func(opt *Options) (err error) {
		opt.protocol = NewProtocolRabbitMQ(url, exchange, id)
		return
	}
}

// Constructors to main components

func NewStoreInMemory() *inmemory.InMemory {
	return inmemory.New()
}

func NewProtocolChannels() *channels.Channels {
	return channels.New()
}

func NewStorePostgresUrl(connectionUrl string) (*postgres.PGStore, error) {
	return postgres.NewFromUrl(connectionUrl)
}

func NewStorePostgresDB(db *sql.DB) (*postgres.PGStore, error) {
	return postgres.New(db)
}

func NewProtocolRabbitMQ(url, exchange, id string) *rabbitmq.Protocol {
	return rabbitmq.New(rabbitmq.Config{
		AmqpUrl:  url,
		Exchange: exchange,
		Id:       id,
	})
}
