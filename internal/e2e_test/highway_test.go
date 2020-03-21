package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-po/po"
	"github.com/go-po/po/internal/broker"
	"github.com/go-po/po/internal/broker/channels"
	"github.com/go-po/po/internal/broker/rabbitmq"
	"github.com/go-po/po/internal/store/inmemory"
	"github.com/go-po/po/internal/store/postgres"
	"github.com/go-po/po/stream"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

const (
	databaseUrl = "postgres://po:po@localhost:5431/po?sslmode=disable"
	uri         = "amqp://po:po@localhost:5671/"
)

type highwayTestCase struct {
	name     string
	store    func() (po.Store, error)     // constructor
	protocol func(id int) broker.Protocol // constructor
	apps     int                          // number of concurrent apps
	subs     int                          // number of subscribers per app
	cars     int                          // number of cars per app
	timeout  time.Duration
}

func TestHighwayApp(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	rand.Seed(time.Now().UnixNano())

	pg := func() func() (po.Store, error) {
		return func() (po.Store, error) {
			return postgres.NewFromUrl(databaseUrl)
		}
	}

	inmem := func() func() (po.Store, error) {
		return func() (store po.Store, err error) {
			return inmemory.New(), nil
		}
	}

	rabbit := func() func(id int) broker.Protocol {
		return func(id int) broker.Protocol {
			return rabbitmq.New(rabbitmq.Config{
				AmqpUrl:  uri,
				Exchange: "highway",
				Id:       fmt.Sprintf("app-%d", id),
			})
		}
	}

	channels := func() func(id int) broker.Protocol {
		return func(id int) broker.Protocol {
			return channels.New()
		}
	}

	tests := []*highwayTestCase{
		{name: "one consumer",
			store: pg(), protocol: rabbit(), apps: 1, subs: 1, cars: 10, timeout: time.Second * 5},
		{name: "multi consumer",
			store: pg(), protocol: rabbit(), apps: 5, subs: 2, cars: 10, timeout: time.Second * 5},
		{name: "channel broker",
			store: pg(), protocol: channels(), apps: 1, subs: 2, cars: 10, timeout: time.Second * 2},
		{name: "inmemory/channel",
			store: inmem(), protocol: channels(), apps: 1, subs: 5, cars: 10, timeout: time.Second},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// setup the shared values for the test
			testId := strconv.Itoa(rand.Int())
			streamId := stream.ParseId("highways:" + testId)
			expected := test.cars * test.apps
			hwCounters, wg := newHighwayCounter(t, test.subs, test.timeout, expected, streamId)

			// setup the apps
			for appId := 0; appId < test.apps; appId++ {
				app := &highwayApp{
					id:       appId,
					streamId: streamId,
					counters: hwCounters,
					test:     test,
				}
				go app.start(t)
			}

			wg.Wait()

			// verify the tests
			for id, counter := range hwCounters {
				assert.Equal(t, expected, counter.Count(), "sub %s", id)
			}

			// prepare po for Projecting
			store, err := test.store()
			if !assert.NoError(t, err, "setup store") {
				t.FailNow()
			}
			es := po.New(store, test.protocol(1000))
			projection := &CarProjection{
				name:  "car-projection-" + testId,
				Cars:  make(map[int64]float64),
				Count: 0,
			}
			err = es.Project(context.Background(), streamId.String(), projection)

			assert.NoError(t, err, "projecting")
			assert.Equal(t, expected, projection.Count, "projection count")

		})
	}
}

type highwayApp struct {
	id       int
	streamId stream.Id
	counters highwayCounters
	test     *highwayTestCase
}

func (app *highwayApp) start(t *testing.T) {
	store, err := app.test.store()
	if !assert.NoError(t, err, "setup store") {
		t.FailNow()
	}

	es := po.New(store, app.test.protocol(app.id))

	for subId, counter := range app.counters {
		err = es.Subscribe(context.Background(), subId, app.streamId.String(), counter)
		if !assert.NoError(t, err, "setup subscriber [%d].[%s]", app.id, subId) {
			t.FailNow()
		}
	}

	// send cars
	appStream := fmt.Sprintf("%s-app-%d", app.streamId, app.id)
	for i := 0; i < app.test.cars; i++ {
		err = es.Stream(context.Background(), appStream).
			Append(Car{Speed: float64(rand.Int31n(100))})
		if !assert.NoError(t, err, "send car [%d].[%s]", app.id, appStream) {
			t.FailNow()
		}
	}

}

type Car struct {
	Speed float64
}

func init() {
	po.RegisterMessages(
		func(b []byte) (interface{}, error) {
			msg := Car{}
			err := json.Unmarshal(b, &msg)
			return msg, err
		},
	)
}

func newHighwayCounter(t *testing.T, count int, timeout time.Duration, expected int, id stream.Id) (highwayCounters, *sync.WaitGroup) {
	hw := make(map[string]*CarCounter)
	timeoutCtx, cancelFn := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancelFn)
	wg := &sync.WaitGroup{}
	for i := 0; i < count; i++ {
		subId := fmt.Sprintf("%s-%d", id, i)
		c := &CarCounter{}
		hw[subId] = c
		wg.Add(1)
		go func() {
			for {
				select {
				case <-timeoutCtx.Done():
					t.Logf("timeout reached for counter: %s", subId)
					t.Fail()
					wg.Done()
					return
				default:
					if c.Count() == expected {
						wg.Done()
						return
					}
					time.Sleep(100 * time.Millisecond)
				}
			}
		}()
	}
	return hw, wg
}

type highwayCounters map[string]*CarCounter

type CarCounter struct {
	mu    sync.Mutex
	count int
}

func (counter *CarCounter) Handle(ctx context.Context, msg stream.Message) error {
	counter.mu.Lock()
	defer counter.mu.Unlock()
	switch msg.Data.(type) {
	case Car:
		counter.count = counter.count + 1
	}
	return nil
}

func (counter *CarCounter) Count() int {
	counter.mu.Lock()
	defer counter.mu.Unlock()
	return counter.count
}

type CarProjection struct {
	name  string
	Cars  map[int64]float64 `json:"cars"`
	Count int               `json:"count"`
}

func (projection *CarProjection) Handle(ctx context.Context, msg stream.Message) error {
	switch car := msg.Data.(type) {
	case Car:
		projection.Cars[msg.Number] = car.Speed
		projection.Count = projection.Count + 1
	default:
		return fmt.Errorf("unknown type: %T", msg)
	}
	return nil
}

func (projection *CarProjection) SnapshotName() string {
	return projection.name
}
