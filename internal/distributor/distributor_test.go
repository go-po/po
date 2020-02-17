package distributor

import (
	"context"
	"encoding/json"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/registry"
	"github.com/go-po/po/stream"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestDistributor_Distribute(t *testing.T) {

	messageRegistry := registry.New()

	messageRegistry.Register(
		func(b []byte) (interface{}, error) {
			msg := TestMessage{}
			err := json.Unmarshal(b, &msg)
			return msg, err
		},
	)

	type deps struct {
		groupNumbers *stubGroupNumberAssigner
	}
	type subscription struct {
		stream string
		sub    *stubDistributorSub
	}
	type subs map[string]subscription
	getSub := func(t *testing.T, subs subs, id string) subscription {
		sub, found := subs[id]
		if !found {
			t.Logf("missing subscription: %s", id)
			t.FailNow()
		}
		return sub
	}
	type distResponse struct {
		ack bool
		err error
	}
	type verify func(t *testing.T, responses []distResponse, errs []error, subs subs, deps deps)
	all := func(v ...verify) []verify { return v }
	noRegisterErr := func() verify {
		return func(t *testing.T, responses []distResponse, errs []error, subs subs, deps deps) {
			assert.Empty(t, errs, "register errors")
		}
	}
	noDistErr := func() verify {
		return func(t *testing.T, responses []distResponse, errs []error, subs subs, deps deps) {
			for i, resp := range responses {
				assert.NoError(t, resp.err, "dist (%d) error", i)
			}
		}
	}
	subGotMessage := func(subId string, number int64, stream string) verify {
		return func(t *testing.T, responses []distResponse, errs []error, subs subs, deps deps) {
			sub := getSub(t, subs, subId)
			var found = false
			for _, msg := range sub.sub.msgs {
				if msg.Stream == stream && msg.Number == number {
					found = true
				}
			}
			assert.True(t, found, "sub got message")
		}
	}
	subGotMessageData := func(subId string, number int64, stream string, data interface{}) verify {
		return func(t *testing.T, responses []distResponse, errs []error, subs subs, deps deps) {
			sub := getSub(t, subs, subId)
			var found = false
			for _, msg := range sub.sub.msgs {
				if msg.Stream == stream && msg.Number == number {
					found = true
					assert.Equal(t, data, msg.Data, "sub got message data content")
				}
			}
			assert.True(t, found, "sub got message data")
		}
	}
	distAccept := func(dist int, expected bool) verify {
		return func(t *testing.T, responses []distResponse, errs []error, subs subs, deps deps) {
			assert.Equal(t, expected, responses[dist].ack, "dist accept")
		}
	}
	tests := []struct {
		name    string
		deps    deps
		subs    map[string]subscription
		records []record.Record
		verify  []verify
	}{
		{
			name:    "no records send",
			subs:    map[string]subscription{},
			records: nil,
			verify: all(
				noRegisterErr(),
				noDistErr(),
			),
		},
		{
			name: "one subscriber - one matching record",
			subs: map[string]subscription{
				"sub": {
					stream: "test stream",
					sub:    &stubDistributorSub{},
				},
			},
			records: []record.Record{
				{
					Number:      1,
					Stream:      "test stream",
					Data:        []byte(`{ "Foo" : "Bar"}`),
					Type:        "distributor.TestMessage",
					GroupNumber: 0,
					Time:        time.Now(),
				},
			},
			verify: all(
				noRegisterErr(),
				noDistErr(),
				subGotMessage("sub", 1, "test stream"),
				subGotMessageData("sub", 1, "test stream", TestMessage{Foo: "Bar"}),
				distAccept(0, true),
			),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// setup
			if test.deps.groupNumbers == nil {
				test.deps.groupNumbers = &stubGroupNumberAssigner{}
			}
			dist := New(test.deps.groupNumbers, messageRegistry)
			var errs []error
			for id, s := range test.subs {
				err := dist.Register(context.Background(), id, stream.ParseId(s.stream), s.sub)
				if err != nil {
					errs = append(errs, err)
				}
			}

			// execute
			var responses []distResponse
			for _, record := range test.records {
				r := distResponse{}
				r.ack, r.err = dist.Distribute(context.Background(), record)
				responses = append(responses, r)
			}

			// verify
			for _, v := range test.verify {
				v(t, responses, errs, test.subs, test.deps)
			}

		})
	}
}

type TestMessage struct {
	Foo string
}

type stubDistributorSub struct {
	msgs []stream.Message
	err  error
}

func (stub *stubDistributorSub) Handle(ctx context.Context, msg stream.Message) error {
	stub.msgs = append(stub.msgs, msg)
	return stub.err
}

type stubGroupNumberAssigner struct {
	assigned []record.Record
	fn       func(r record.Record) (int64, error)
	err      error
	number   int64
}

func (stub *stubGroupNumberAssigner) AssignGroupNumber(ctx context.Context, r record.Record) (int64, error) {
	stub.assigned = append(stub.assigned, r)
	if stub.fn == nil {
		return stub.number, stub.err
	}
	return stub.fn(r)
}
