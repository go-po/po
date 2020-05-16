package distributor

import (
	"context"
	"errors"
	"testing"

	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/streams"
	"github.com/stretchr/testify/assert"
)

var ErrTest = errors.New("test error")

func TestRecordingSubscription_Handle(t *testing.T) {
	type deps struct {
		handler *stubHandler
		store   *stubMsgStore
	}
	type verify func(t *testing.T, err error, deps deps)
	all := func(v ...verify) []verify { return v }

	anErr := func(expected error) verify {
		return func(t *testing.T, err error, deps deps) {
			assert.Equal(t, expected, err, "an error")
		}
	}

	noErr := func() verify {
		return func(t *testing.T, err error, deps deps) {
			assert.NoError(t, err, "no error")
		}
	}
	gotMessageNumber := func(expected int64) verify {
		return func(t *testing.T, err error, deps deps) {
			var got []int64
			for _, msg := range deps.handler.msgs {
				got = append(got, msg.Number)
			}
			assert.Contains(t, got, expected, "message number %d not found", expected)
		}
	}
	gotMessageCount := func(expected int) verify {
		return func(t *testing.T, err error, deps deps) {
			assert.Equal(t, expected, len(deps.handler.msgs), "message count")
		}
	}

	tests := []struct {
		name        string
		deps        deps
		msg         streams.Message
		groupStream bool
		verify      []verify
	}{
		{
			name: "failing handler",
			deps: deps{
				handler: &stubHandler{err: ErrTest},
			},
			msg: streams.Message{Number: 1},
			verify: all(
				anErr(ErrTest),
			),
		},
		{
			name: "failing store",
			deps: deps{
				store: &stubMsgStore{err: ErrTest},
			},
			msg: streams.Message{Number: 1},
			verify: all(
				anErr(ErrTest),
			),
		},
		{
			name: "first",
			deps: deps{
				store: &stubMsgStore{
					lastPosition: 0,
				},
			},
			msg:         streams.Message{Number: 1},
			groupStream: false,
			verify: all(
				noErr(),
				gotMessageNumber(1),
			),
		},
		{
			name: "ignored",
			deps: deps{
				store: &stubMsgStore{
					lastPosition: 100,
				},
			},
			msg:         streams.Message{Number: 100},
			groupStream: false,
			verify: all(
				noErr(),
				gotMessageCount(0),
			),
		},
		{
			name: "missing in between",
			deps: deps{
				store: &stubMsgStore{
					lastPosition: 1,
					msg:          []streams.Message{{Number: 1}, {Number: 2}, {Number: 3}},
				},
			},
			msg:         streams.Message{Number: 3},
			groupStream: false,
			verify: all(
				noErr(),
				gotMessageCount(2),
				gotMessageNumber(2),
				gotMessageNumber(3),
			),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// setup
			if test.deps.handler == nil {
				test.deps.handler = &stubHandler{}
			}
			if test.deps.store == nil {
				test.deps.store = &stubMsgStore{}
			}
			sub := &recordingSubscription{
				handler:     test.deps.handler,
				store:       test.deps.store,
				groupStream: test.groupStream,
			}
			// execute
			err := sub.Handle(context.Background(), test.msg)

			// verify
			for _, v := range test.verify {
				v(t, err, test.deps)
			}
		})
	}
}

type stubMsgStore struct {
	err          error
	lastPosition int64
	position     int64
	msg          []streams.Message
}

func (stub *stubMsgStore) Begin(ctx context.Context) (store.Tx, error) {
	return stubTx{}, stub.err
}

func (stub *stubMsgStore) GetLastPosition(tx store.Tx) (int64, error) {
	return stub.lastPosition, stub.err
}

func (stub *stubMsgStore) SetPosition(tx store.Tx, position int64) error {
	stub.position = position
	return stub.err
}

func (stub *stubMsgStore) ReadMessages(ctx context.Context, from int64) ([]streams.Message, error) {
	return stub.msg[from:], stub.err
}

type stubTx struct {
	err error
}

func (stub stubTx) Commit() error {
	return stub.err
}

func (stub stubTx) Rollback() error {
	return stub.err
}
