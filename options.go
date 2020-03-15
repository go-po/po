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
	store          Store
	brokerProtocol broker.Protocol
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

func WithBrokerChannel() Option {
	return func(opt *Options) error {
		opt.brokerProtocol = channels.New()
		return nil
	}
}

func WithBrokerRabbit(url, exchange, id string) Option {
	return func(opt *Options) (err error) {
		opt.brokerProtocol = rabbitmq.New(rabbitmq.Config{
			AmqpUrl:  url,
			Exchange: exchange,
			Id:       id,
		})
		return
	}
}

func New(store Store, broker Broker) *Po {
	dist := distributor.New(registry.DefaultRegistry, store)
	return &Po{
		store:       store,
		broker:      broker,
		distributor: dist,
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
	if options.brokerProtocol == nil {
		return nil, fmt.Errorf("po: no broker protocol provided")
	}

	dist := distributor.New(registry.DefaultRegistry, options.store)
	broker := broker.New(options.brokerProtocol, dist, options.store)

	return New(options.store, broker), nil

}
