package channels

import (
	"context"
	"github.com/kyuff/po/internal/record"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestChannels_Notify(t *testing.T) {
	R := func(records ...record.Record) []record.Record { return records }
	type verify func(t *testing.T, err error)
	all := func(v ...verify) []verify { return v }

	noErr := func() verify {
		return func(t *testing.T, err error) {
			assert.NoError(t, err)
		}
	}
	tests := []struct {
		name    string
		records []record.Record
		verify  []verify
	}{
		{
			name:    "nothing done",
			records: R(),
			verify: all(
				noErr(),
			),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// setup
			ctx := context.Background()
			ch := New()

			// execute
			err := ch.Notify(ctx, test.records...)

			// verify
			for _, v := range test.verify {
				v(t, err)
			}
		})
	}
}
