package po

import (
	"database/sql"
	"fmt"

	"github.com/go-po/po/internal/broker"
	"github.com/go-po/po/internal/broker/channels"
	"github.com/go-po/po/internal/logger"
	"github.com/go-po/po/internal/observer"
	"github.com/go-po/po/internal/registry"
	"github.com/go-po/po/internal/store/inmemory"
	"github.com/go-po/po/internal/store/postgres"
	"github.com/prometheus/client_golang/prometheus"
)

type Options struct {
	store    Store
	registry Registry
	logger   Logger
	prom     prometheus.Registerer
	protocol broker.Protocol
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
	if options.registry == nil {
		return nil, fmt.Errorf("po: no registry provided")
	}
	if options.logger == nil {
		return nil, fmt.Errorf("po: no logger provided")
	}

	if options.protocol == nil {
		return nil, fmt.Errorf("po: no broker protocol provided")
	}

	return newPo(options.store, options.protocol, options.registry, options.logger, builder), nil
}

func newPo(store Store, protocol broker.Protocol, registry Registry, logger Logger, builder *observer.Builder) *Po {
	store = observeStore(store, builder)
	broker := observeBroker(
		broker.New(store, registry, observeProtocol(protocol, builder)),
		builder)
	return &Po{
		obs: poObserver{
			Stream:  builder.Nullary().Build(),
			Project: builder.Nullary().Build(),
		},
		logger:   logger,
		builder:  builder,
		store:    store,
		broker:   broker,
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
		opt.store = store
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
		return err
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

func NewStorePostgresDB(db *sql.DB) (*postgres.Storage, error) {
	return postgres.NewFromConn(db)
}
