package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-po/po"
	"github.com/go-po/po/internal/logger"
	"github.com/go-po/po/internal/observer"
	"github.com/go-po/po/streams"
	"github.com/stretchr/testify/assert"
)

type highwayTestCase struct {
	name     string
	store    StoreBuilder    // constructor
	protocol ProtocolBuilder // constructor
	apps     int             // number of concurrent apps
	subs     int             // number of subscribers per app
	cars     int             // number of cars per app
	timeout  time.Duration
}

func TestHighwayApp(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	rand.Seed(time.Now().UnixNano())

	tests := []*highwayTestCase{
		{name: "one consumer",
			store: pg(), protocol: rabbit(), apps: 1, subs: 1, cars: 10, timeout: time.Second * 5},
		{name: "multi consumer",
			store: pg(), protocol: rabbit(), apps: 5, subs: 2, cars: 10, timeout: time.Second * 5},
		{name: "channel broker",
			store: pg(), protocol: channel(), apps: 1, subs: 2, cars: 10, timeout: time.Second * 2},
		{name: "inmemory/channel",
			store: inmem(), protocol: channel(), apps: 1, subs: 5, cars: 10, timeout: time.Second},
		{name: "inmemory/rabbit",
			store: inmem(), protocol: rabbit(), apps: 1, subs: 5, cars: 10, timeout: time.Second * 5},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// setup the shared values for the test
			testId := strconv.Itoa(rand.Int())
			streamId := streams.ParseId("highways:" + testId)
			expected := test.cars * test.apps
			hwCounters, wg := newHighwayCounter(t, test.subs, test.timeout, expected, streamId)

			// setup the apps
			var apps []*highwayApp
			for appId := 0; appId < test.apps; appId++ {
				app := &highwayApp{
					id:       appId,
					streamId: streamId,
					counters: hwCounters,
					test:     test,
					obs:      observer.New(logger.WrapLogger(t), observer.NewPromStub()),
				}
				apps = append(apps, app)
				go app.start(t)
			}

			wg.Wait()

			// verify the tests
			for id, counter := range hwCounters {
				assert.Equal(t, expected, counter.Count(), "sub %s", id)
			}

			for _, app := range apps {
				app.verifyProjection(t, expected)
			}

		})
	}
}

type highwayApp struct {
	id       int
	streamId streams.Id
	counters highwayCounters
	test     *highwayTestCase
	es       *po.Po
	obs      *observer.Builder
}

func (app *highwayApp) start(t *testing.T) {
	store, err := app.test.store(app.obs)
	if !assert.NoError(t, err, "setup store") {
		t.Fail()
	}

	app.es = po.New(store, app.test.protocol(app.id))

	for subId, counter := range app.counters {
		err = app.es.Subscribe(context.Background(), subId, app.streamId, counter)
		if !assert.NoError(t, err, "setup subscriber [%d].[%s]", app.id, subId) {
			t.Fail()
		}
	}

	// send cars
	appStream := streams.ParseId("%s-app-%d", app.streamId, app.id)
	for i := 0; i < app.test.cars; i++ {
		message := Car{Speed: float64(rand.Int31n(100))}
		_, err = app.es.Stream(context.Background(), appStream).Append(message)
		if !assert.NoError(t, err, "send car [%d].[%s]", app.id, appStream) {
			t.Fail()
		}
	}

}

func (app *highwayApp) verifyProjection(t *testing.T, expected int) {
	projection := &CarProjection{
		name:  "car-projection-" + app.streamId.String(),
		Cars:  make(map[int64]float64),
		Count: 0,
	}
	err := app.es.Project(context.Background(), app.streamId, projection)

	assert.NoError(t, err, "projecting")
	assert.Equal(t, expected, projection.Count, "projection count")
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

func newHighwayCounter(t *testing.T, count int, timeout time.Duration, expected int, id streams.Id) (highwayCounters, *sync.WaitGroup) {
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
	last  int64
}

func (counter *CarCounter) Handle(ctx context.Context, msg streams.Message) error {
	counter.mu.Lock()
	defer counter.mu.Unlock()

	if counter.last+1 > msg.GroupNumber {
		// handle messages with idempotence
		return nil
	}

	counter.last = msg.GroupNumber

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

func (projection *CarProjection) Handle(ctx context.Context, msg streams.Message) error {
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
