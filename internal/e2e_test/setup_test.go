package e2e_test

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/go-po/po"
	"github.com/go-po/po/internal/broker"
	"github.com/go-po/po/internal/broker/channels"
	"github.com/go-po/po/internal/broker/rabbitmq"
	"github.com/go-po/po/internal/store/inmemory"
	"github.com/go-po/po/internal/store/postgres"
	"github.com/go-po/po/streams"
)

const (
	postgresUrl = "postgres://po:po@localhost:5431/po?sslmode=disable"
	rabbitmqUrl = "amqp://po:po@localhost:5671/"
)

type StoreBuilder func() (po.Store, error)

func pg() StoreBuilder {
	return func() (po.Store, error) {
		return postgres.NewFromUrl(postgresUrl)
	}
}

func inmem() StoreBuilder {
	return func() (store po.Store, err error) {
		return inmemory.New(), nil
	}
}

type ProtocolBuilder func(id int) broker.OProtocol

func rabbit() ProtocolBuilder {
	return func(id int) broker.OProtocol {
		return rabbitmq.NewTransport(rabbitmq.Config{
			AmqpUrl:  rabbitmqUrl,
			Exchange: "highway",
			Id:       fmt.Sprintf("app-%d", id),
		}, nil)
	}
}

func channel() ProtocolBuilder {
	return func(id int) broker.OProtocol {
		return channels.New()
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func randStreamId(groupBase string, entity string) streams.Id {
	id := streams.ParseId(groupBase + ":" + strconv.Itoa(rand.Int()))
	id.Entity = entity
	return id
}
