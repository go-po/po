package po

import (
	"database/sql"
	"fmt"

	"github.com/go-po/po/internal/broker"
	"github.com/go-po/po/internal/broker/channels"
	"github.com/go-po/po/internal/broker/rabbitmq"
	"github.com/go-po/po/internal/logger"
	"github.com/go-po/po/internal/observer"
	"github.com/go-po/po/internal/registry"
	"github.com/go-po/po/internal/store/inmemory"
	"github.com/go-po/po/internal/store/postgres"
	"github.com/prometheus/client_golang/prometheus"
)

type storeBuilder func(obs *observer.Builder) (Store, error)

type Options struct {
	store    storeBuilder
	protocol broker.Protocol
	registry Registry
	logger   Logger
	prom     prometheus.Registerer
}

type Option func(opt *Options) error

func New(store Store, protocol broker.Protocol) *Po {
	logger := &logger.NoopLogger{}
	return newPo(
		store,
		protocol,
		registry.DefaultRegistry,
		logger,
		observer.New(logger, observer.NewPromStub()),
	)
}

func NewFromOptions(opts ...Option) (*Po, error) {
	options := &Options{
		registry: registry.DefaultRegistry,
		logger:   &logger.NoopLogger{},
		prom:     observer.NewPromStub(),
	}

	for _, opt := range opts {
		err := opt(options)
		if err != nil {
			return nil, err
		}
	}

	builder := observer.New(options.logger, options.prom)

	if options.store == nil {
		return nil, fmt.Errorf("po: no store provided")
	}
	store, err := options.store(builder)
	if err != nil {
		return nil, err
	}
	if options.protocol == nil {
		return nil, fmt.Errorf("po: no protocol provided")
	}
	if options.registry == nil {
		return nil, fmt.Errorf("po: no registry provided")
	}
	if options.logger == nil {
		return nil, fmt.Errorf("po: no logger provided")
	}

	return newPo(store, options.protocol, options.registry, options.logger, builder), nil
}

func newPo(store Store, protocol broker.Protocol, registry Registry, logger Logger, builder *observer.Builder) *Po {
	return &Po{
		obs: poObserver{
			Stream:  builder.Nullary().Build(),
			Project: builder.Nullary().Build(),
		},
		logger:   logger,
		builder:  builder,
		store:    store,
		broker:   broker.New(store, registry, protocol),
		registry: registry,
	}
}

// Available options

func WithPrometheus(prom prometheus.Registerer) Option {
	return func(opt *Options) error {
		opt.prom = prom
		return nil
	}
}

func WithLogger(logger Logger) Option {
	return func(opt *Options) error {
		opt.logger = logger
		return nil
	}
}

func WithStore(store Store) Option {
	return func(opt *Options) error {
		opt.store = func(obs *observer.Builder) (Store, error) {
			return store, nil
		}
		return nil
	}
}

func WithProtocol(protocol broker.Protocol) Option {
	return func(opt *Options) error {
		opt.protocol = protocol
		return nil
	}
}

func WithStoreInMemory() Option {
	return func(opt *Options) error {
		opt.store = func(_ *observer.Builder) (Store, error) {
			return NewStoreInMemory(), nil
		}
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
		opt.store = func(obs *observer.Builder) (Store, error) {
			return NewStorePostgresUrl(connectionUrl)
		}
		return
	}
}

func WithStorePostgresDB(db *sql.DB) Option {
	return func(opt *Options) error {
		opt.store = func(obs *observer.Builder) (Store, error) {
			return NewStorePostgresDB(db, obs)
		}
		return nil
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

func NewStorePostgresUrl(connectionUrl string) (*postgres.Storage, error) {
	return postgres.NewFromUrl(connectionUrl)
}

func NewStorePostgresDB(db *sql.DB, obs *observer.Builder) (*postgres.Storage, error) {
	return postgres.NewFromConn(db), nil
}

func NewProtocolRabbitMQ(url, exchange, id string) *rabbitmq.Transport {
	return rabbitmq.NewTransport(rabbitmq.Config{
		AmqpUrl:  url,
		Exchange: exchange,
		Id:       id,
	}, nil)
}
