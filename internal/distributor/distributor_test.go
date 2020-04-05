package distributor

import (
	"context"
	"encoding/json"
	"github.com/go-po/po/internal/record"
	defaultRegistry "github.com/go-po/po/internal/registry"
	"github.com/go-po/po/streams"
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

	type deps struct{}

	type verify func(t *testing.T, ack bool, errDist error, subs map[string][]*stubHandler, deps deps)
	all := func(v ...verify) []verify { return v }
	noErr := func() verify {
		return func(t *testing.T, ack bool, errDist error, subs map[string][]*stubHandler, deps deps) {
			assert.NoError(t, errDist, "no err")
		}
	}

	type verifyMessage func(t *testing.T, msg streams.Message)
	msgNumber := func(expected int64) verifyMessage {
		return func(t *testing.T, msg streams.Message) {
			assert.Equal(t, expected, msg.Number, "message number")
		}
	}
	msgData := func(expected interface{}) verifyMessage {
		return func(t *testing.T, msg streams.Message) {
			assert.Equal(t, expected, msg.Data, "message data content")
		}
	}
	getSub := func(t *testing.T, subs map[string][]*stubHandler, subId string) *stubHandler {
		for _, handlers := range subs {
			for _, handler := range handlers {
				if handler.name == subId {
					return handler
				}
			}
		}
		t.Logf("sub id not declared: %s", subId)
		t.FailNow()
		return nil
	}
	subGotCount := func(subId string, expected int) verify {
		return func(t *testing.T, ack bool, errDist error, subs map[string][]*stubHandler, deps deps) {
			sub := getSub(t, subs, subId)
			if !assert.Equal(t, expected, len(sub.msgs), "sub count") {
				t.FailNow()
			}
		}
	}
	subGot := func(subId string, num int, expect ...verifyMessage) verify {
		return func(t *testing.T, ack bool, errDist error, subs map[string][]*stubHandler, deps deps) {
			sub := getSub(t, subs, subId)
			if assert.True(t, num < len(sub.msgs), "sub got too few messages: %s", subId) {
				for _, v := range expect {
					v(t, sub.msgs[num])
				}
			}
		}
	}

	distAccept := func(dist int, expected bool) verify {
		return func(t *testing.T, ack bool, errDist error, subs map[string][]*stubHandler, deps deps) {
			assert.Equal(t, expected, ack, "dist accept")
		}
	}
	DefaultTestRecord := record.Record{
		Number:      1,
		Stream:      streams.ParseId("test stream"),
		Data:        []byte(`{ "Foo" : "Bar"}`),
		Group:       "",
		ContentType: "application/json;type=distributor.TestMessage",
		GroupNumber: 0,
		Time:        time.Now(),
	}
	tests := map[string]struct {
		deps   deps
		subs   map[string][]*stubHandler
		record record.Record
		verify []verify
	}{
		"one subscriber - one matching record": {

			subs: map[string][]*stubHandler{
				"test stream": {
					{name: "sub"},
				},
			},
			record: DefaultTestRecord,
			verify: all(
				noErr(),
				subGotCount("sub", 1),
				subGot("sub", 0,
					msgNumber(1),
					msgData(TestMessage{Foo: "Bar"}),
				),
				distAccept(0, true),
			),
		},
		"two subscriber - one matching record": {

			subs: map[string][]*stubHandler{
				"test stream": {
					{name: "sub 1"}, {name: "sub 2"},
				},
			},
			record: DefaultTestRecord,
			verify: all(
				noErr(),
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
		"two subscriber - different streams": {

			subs: map[string][]*stubHandler{
				"test stream": {
					{name: "sub 1"},
				},
				"different stream": {
					{name: "sub 2"},
				},
			},
			record: DefaultTestRecord,
			verify: all(
				noErr(),
				subGotCount("sub 1", 1),
				subGot("sub 1", 0,
					msgNumber(1),
					msgData(TestMessage{Foo: "Bar"}),
				),
				subGotCount("sub 2", 0),
				distAccept(0, true),
			),
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			// setup

			dist := New(messageRegistry, nil)
			for id, subs := range test.subs {
				for _, sub := range subs {
					dist.subs[id] = append(dist.subs[id], sub)
				}
			}

			// execute
			ack, distErr := dist.Distribute(context.Background(), test.record)

			// verify
			for _, v := range test.verify {
				v(t, ack, distErr, test.subs, test.deps)
			}

		})
	}
}

type stubHandler struct {
	name string
	msgs []streams.Message
	err  error
}

func (stub *stubHandler) Handle(ctx context.Context, msg streams.Message) error {
	stub.msgs = append(stub.msgs, msg)
	return stub.err
}
