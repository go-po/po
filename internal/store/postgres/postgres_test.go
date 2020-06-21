package postgres

import (
	"context"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/streams"
	"github.com/stretchr/testify/assert"
)

func TestPGStore_StoreRecord(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	rand.Seed(time.Now().UnixNano())
	type verify func(t *testing.T, r record.Record, err error)
	type call struct {
		id     string
		number int64
		verify []verify
	}
	all := func(v ...verify) []verify { return v }
	noErr := func() verify {
		return func(t *testing.T, r record.Record, err error) {
			assert.NoError(t, err, "no error")
		}
	}
	anErr := func() verify {
		return func(t *testing.T, r record.Record, err error) {
			assert.Error(t, err, "an error")
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
				{id: "group-entity", number: 1, verify: all(
					noErr(),
					number(1),
					groupNumber(0),
				)},
			},
		},
		{
			name: "two record / entity stream",
			calls: []call{
				{id: "group-entity", number: 1, verify: all(
					noErr(),
					number(1),
					groupNumber(0),
				)},
				{id: "group-entity", number: 2, verify: all(
					noErr(),
					number(2),
					groupNumber(0),
				)},
			},
		},
		{
			name: "two record / two entity streams",
			calls: []call{
				{id: "group-entity-a", number: 1, verify: all(
					noErr(),
					number(1),
					groupNumber(0),
				)},
				{id: "group-entity-b", number: 1, verify: all(
					noErr(),
					number(1),
					groupNumber(0),
				)},
			},
		},
		{
			name: "one record / group stream",
			calls: []call{
				{id: "group", number: 1, verify: all(
					noErr(),
					number(1),
					groupNumber(0),
				)},
			},
		},
		{
			name: "two record / group stream",
			calls: []call{
				{id: "group", number: 1, verify: all(
					noErr(),
					number(1),
					groupNumber(0),
				)},
				{id: "group", number: 2, verify: all(
					noErr(),
					number(2),
					groupNumber(0),
				)},
			},
		},
		{
			name: "new stream / out of order",
			calls: []call{
				{id: "group", number: 3, verify: all(
					anErr(),
				)},
			},
		},
		{
			name: "out of order",
			calls: []call{
				{id: "group", number: 1, verify: all(
					noErr(),
					number(1),
					groupNumber(0),
				)},
				{id: "group", number: 3, verify: all(
					anErr(),
				)},
			},
		},
		{
			name: "multiple to same number",
			calls: []call{
				{id: "group", number: 1, verify: all(
					noErr(),
					number(1),
					groupNumber(0),
				)},
				{id: "group", number: 1, verify: all(
					anErr(),
				)},
			},
		},
		{
			name: "zero number",
			calls: []call{
				{id: "group", number: 0, verify: all(
					anErr(),
				)},
			},
		},
		{
			name: "negative number",
			calls: []call{
				{id: "group", number: -1, verify: all(
					anErr(),
				)},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := connect(t)
			tx, err := s.Begin(context.Background())
			if !assert.NoError(t, err, "begin tx") {
				t.FailNow()
			}
			// to not have collisions with multiple runs of the test
			prefix := strconv.FormatInt(rand.Int63(), 10)
			for _, call := range test.calls {
				id := streams.ParseId(prefix + call.id)
				r, err := s.StoreRecord(tx, id, call.number, "text/plain", []byte("data: "+call.id))
				for _, v := range call.verify {
					v(t, r, err)
				}
			}
			err = tx.Commit()
			assert.NoError(t, err, "commit test case")
		})
	}
}
