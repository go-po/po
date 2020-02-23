package distributor

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-po/po/internal/record"
	defaultRegistry "github.com/go-po/po/internal/registry"
	"github.com/go-po/po/stream"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type TestMessage struct {
	Foo string
}

type UnregisteredTestMessage struct {
	Foo string
}

func TestDistributor_Distribute(t *testing.T) {

	messageRegistry := defaultRegistry.New()

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
	type verify func(t *testing.T, ack bool, errDist error, errs []error, subs subs, deps deps)
	all := func(v ...verify) []verify { return v }
	noErrRegister := func() verify {
		return func(t *testing.T, ack bool, errDist error, errs []error, subs subs, deps deps) {
			assert.Empty(t, errs, "register errors")
		}
	}
	noErrDist := func() verify {
		return func(t *testing.T, ack bool, errDist error, errs []error, subs subs, deps deps) {
			assert.NoError(t, errDist, "no dist err")
		}
	}
	anErrDist := func() verify {
		return func(t *testing.T, ack bool, errDist error, errs []error, subs subs, deps deps) {
			assert.Error(t, errDist, "an err dist")
		}
	}
	type verifyMessage func(t *testing.T, msg stream.Message)
	msgNumber := func(expected int64) verifyMessage {
		return func(t *testing.T, msg stream.Message) {
			assert.Equal(t, expected, msg.Number, "message number")
		}
	}
	msgData := func(expected interface{}) verifyMessage {
		return func(t *testing.T, msg stream.Message) {
			assert.Equal(t, expected, msg.Data, "message data content")
		}
	}
	msgGroupNumber := func(expected int64) verifyMessage {
		return func(t *testing.T, msg stream.Message) {
			assert.Equal(t, expected, msg.GroupNumber, "message group number")
		}
	}
	subGotCount := func(subId string, expected int) verify {
		return func(t *testing.T, ack bool, errDist error, errs []error, subs subs, deps deps) {
			sub := getSub(t, subs, subId)
			if !assert.Equal(t, expected, len(sub.sub.msgs), "sub count") {
				t.FailNow()
			}
		}
	}
	subGot := func(subId string, num int, expect ...verifyMessage) verify {
		return func(t *testing.T, ack bool, errDist error, errs []error, subs subs, deps deps) {
			sub := getSub(t, subs, subId)
			if assert.True(t, num < len(sub.sub.msgs), "sub got too few messages: %s", subId) {
				for _, v := range expect {
					v(t, sub.sub.msgs[num])
				}
			}
		}
	}

	distAccept := func(dist int, expected bool) verify {
		return func(t *testing.T, ack bool, errDist error, errs []error, subs subs, deps deps) {
			assert.Equal(t, expected, ack, "dist accept")
		}
	}
	DefaultTestRecord := record.Record{
		Number:      1,
		Stream:      stream.ParseId("test stream"),
		Data:        []byte(`{ "Foo" : "Bar"}`),
		Group:       "distributor.TestMessage",
		GroupNumber: 0,
		Time:        time.Now(),
	}
	tests := map[string]struct {
		deps   deps
		subs   map[string]subscription
		record record.Record
		verify []verify
	}{
		"one subscriber - one matching record": {

			subs: map[string]subscription{
				"sub": {
					stream: "test stream",
					sub:    &stubDistributorSub{},
				},
			},
			record: DefaultTestRecord,
			verify: all(
				noErrRegister(),
				noErrDist(),
				subGotCount("sub", 1),
				subGot("sub", 0,
					msgNumber(1),
					msgData(TestMessage{Foo: "Bar"}),
				),
				distAccept(0, true),
			),
		},
		"two subscriber - one matching record": {

			subs: map[string]subscription{
				"sub 1": {
					stream: "test stream",
					sub:    &stubDistributorSub{},
				},
				"sub 2": {
					stream: "test stream",
					sub:    &stubDistributorSub{},
				},
			},
			record: DefaultTestRecord,
			verify: all(
				noErrRegister(),
				noErrDist(),
				subGotCount("sub 1", 1),
				subGot("sub 1", 0,
					msgNumber(1),
					msgData(TestMessage{Foo: "Bar"}),
				),
				subGotCount("sub 2", 1),
				subGot("sub 2", 0,
					msgNumber(1),
					msgData(TestMessage{Foo: "Bar"}),
				),
				distAccept(0, true),
			),
		},
		"assigned group number": {
			deps: deps{
				groupNumbers: &stubGroupNumberAssigner{
					fn: func(r record.Record) (int64, error) {
						return 5, nil
					},
				},
			},
			subs: map[string]subscription{
				"sub": {
					stream: "test stream",
					sub:    &stubDistributorSub{},
				},
			},
			record: DefaultTestRecord,
			verify: all(
				noErrRegister(),
				noErrDist(),
				subGotCount("sub", 1),
				subGot("sub", 0,
					msgNumber(1),
					msgData(TestMessage{Foo: "Bar"}),
					msgGroupNumber(5),
				),
				distAccept(0, true),
			),
		},
		"failed assigning numbers": {
			deps: deps{
				groupNumbers: &stubGroupNumberAssigner{
					err: fmt.Errorf("failed assigning"),
				},
			},
			subs: map[string]subscription{
				"sub": {
					stream: "test stream",
					sub:    &stubDistributorSub{},
				},
			},
			record: DefaultTestRecord,
			verify: all(
				noErrRegister(),
				anErrDist(),
				subGotCount("sub", 0),
				distAccept(0, false),
			),
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
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
			ack, distErr := dist.Distribute(context.Background(), test.record)

			// verify
			for _, v := range test.verify {
				v(t, ack, distErr, errs, test.subs, test.deps)
			}

		})
	}
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
