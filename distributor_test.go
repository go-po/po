package po

import (
	"context"
	"encoding/json"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/stream"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestDistributor_Distribute(t *testing.T) {
	type deps struct{}
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
	type testErrors struct {
		dist     []error
		register []error
	}
	type verify func(t *testing.T, errs testErrors, subs subs, deps deps)
	all := func(v ...verify) []verify { return v }
	noRegisterErr := func() verify {
		return func(t *testing.T, errs testErrors, subs subs, deps deps) {
			assert.Empty(t, errs.register, "register errors")
		}
	}
	noDistErr := func() verify {
		return func(t *testing.T, errs testErrors, subs subs, deps deps) {
			assert.Empty(t, errs.dist, "dist errors")
		}
	}
	subGotMessage := func(subId string, number int64, stream string) verify {
		return func(t *testing.T, errs testErrors, subs subs, deps deps) {
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
		return func(t *testing.T, errs testErrors, subs subs, deps deps) {
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
					Type:        "po.TestMessage",
					GroupNumber: 0,
					Time:        time.Now(),
				},
			},
			verify: all(
				noRegisterErr(),
				noDistErr(),
				subGotMessage("sub", 1, "test stream"),
				subGotMessageData("sub", 1, "test stream", TestMessage{Foo: "Bar"}),
			),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// setup
			dist := newDistributor()
			var errs testErrors
			for id, s := range test.subs {
				err := dist.Register(context.Background(), id, stream.ParseId(s.stream), s.sub)
				if err != nil {
					errs.register = append(errs.register, err)
				}
			}

			// execute
			for _, record := range test.records {
				err := dist.Distribute(context.Background(), record)
				if err != nil {
					errs.dist = append(errs.dist, err)
				}
			}

			// verify
			for _, v := range test.verify {
				v(t, errs, test.subs, test.deps)
			}

		})
	}
}

type TestMessage struct {
	Foo string
}

type stubDistributorSub struct {
	msgs []Message
	err  error
}

func init() {
	RegisterMessages(
		func(b []byte) (interface{}, error) {
			msg := TestMessage{}
			err := json.Unmarshal(b, &msg)
			return msg, err
		},
	)
}

func (stub *stubDistributorSub) Handle(ctx context.Context, msg Message) error {
	stub.msgs = append(stub.msgs, msg)
	return stub.err
}
