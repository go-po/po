package po

import (
	"database/sql"
	"fmt"
	"github.com/go-po/po/internal/broker/channels"
	"github.com/go-po/po/internal/distributor"
	"github.com/go-po/po/internal/registry"
	"github.com/go-po/po/internal/store/inmemory"
	"github.com/go-po/po/internal/store/postgres"
)

type Options struct {
	store  Store
	broker Broker
}

type Option func(opt *Options) error

func WithInMemoryStore() Option {
	return func(opt *Options) error {
		opt.store = inmemory.New()
		return nil
	}
}

func WithPGDatabaseUrlStore(connectionUrl string) Option {
	return func(opt *Options) (err error) {
		opt.store, err = postgres.NewFromUrl(connectionUrl)
		return
	}
}

func WithPGConnection(db *sql.DB) Option {
	return func(opt *Options) (err error) {
		opt.store, err = postgres.New(db)
		return
	}
}

func WithChannelBroker() Option {
	return func(opt *Options) error {
		opt.broker = channels.New()
		return nil
	}
}

func New(store Store, broker Broker) *Po {
	dist := distributor.New(registry.DefaultRegistry, store)
	broker.Prepare(dist, store)
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

	if options.broker == nil {
		return nil, fmt.Errorf("po: no broker provided")
	}

	if options.store == nil {
		return nil, fmt.Errorf("po: no store provided")
	}

	return New(options.store, options.broker), nil

}
