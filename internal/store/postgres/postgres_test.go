// +build integration

package postgres

import (
	"context"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/stream"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

const databaseUrl = "postgres://po:po@localhost:5431/po?sslmode=disable"

func connect(t *testing.T) *PGStore {
	t.Helper()
	store, err := NewFromUrl(databaseUrl)
	if !assert.NoError(t, err, "connecting") {
		t.FailNow()
	}
	return store
}

func TestPGStore_StoreRecord(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	type verify func(t *testing.T, r record.Record, err error)
	type call struct {
		id     string
		verify []verify
	}
	all := func(v ...verify) []verify { return v }
	noErr := func() verify {
		return func(t *testing.T, r record.Record, err error) {
			assert.NoError(t, err, "no error")
		}
	}
	number := func(expected int64) verify {
		return func(t *testing.T, r record.Record, err error) {
			assert.Equal(t, expected, r.Number, "number")
		}
	}
	groupNumber := func(expected int64) verify {
		return func(t *testing.T, r record.Record, err error) {
			assert.Equal(t, expected, r.GroupNumber, "group number")
		}
	}
	tests := []struct {
		name  string
		calls []call
	}{
		{
			name: "one record / entity stream",
			calls: []call{
				{id: "group-entity", verify: all(
					noErr(),
					number(1),
					groupNumber(0),
				)},
			},
		},
		{
			name: "two record / entity stream",
			calls: []call{
				{id: "group-entity", verify: all(
					noErr(),
					number(1),
					groupNumber(0),
				)},
				{id: "group-entity", verify: all(
					noErr(),
					number(2),
					groupNumber(0),
				)},
			},
		},
		{
			name: "one record / group stream",
			calls: []call{
				{id: "group", verify: all(
					noErr(),
					number(1),
					groupNumber(1),
				)},
			},
		},
		{
			name: "two record / group stream",
			calls: []call{
				{id: "group", verify: all(
					noErr(),
					number(1),
					groupNumber(1),
				)},
				{id: "group", verify: all(
					noErr(),
					number(2),
					groupNumber(2),
				)},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := connect(t)
			tx, err := store.Begin(context.Background())
			if !assert.NoError(t, err, "begin tx") {
				t.FailNow()
			}
			// to not have collisions with multiple runs of the test
			prefix := strconv.FormatInt(rand.Int63(), 10)
			for _, call := range test.calls {
				id := stream.ParseId(prefix + call.id)
				r, err := store.StoreRecord(tx, id, "text/plain", []byte("data: "+call.id))
				for _, v := range call.verify {
					v(t, r, err)
				}
			}
			err = tx.Commit()
			assert.NoError(t, err, "commit test case")
		})
	}
}
