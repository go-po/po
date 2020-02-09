package po

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseStreamId(t *testing.T) {
	tests := []struct {
		input  string
		expect StreamId
	}{
		{input: "users", expect: StreamId{Type: "users", Entity: ""}},
		{input: "users:commands", expect: StreamId{Type: "users:commands", Entity: ""}},
		{input: "users-peter", expect: StreamId{Type: "users", Entity: "peter"}},
		{input: "users-peter-peter", expect: StreamId{Type: "users", Entity: "peter-peter"}},
		{input: "users-", expect: StreamId{Type: "users", Entity: ""}},
		{input: "-users", expect: StreamId{Type: "", Entity: "users"}},
		{input: "", expect: StreamId{Type: "", Entity: ""}},
		{input: "  users", expect: StreamId{Type: "users", Entity: ""}},
		{input: "users   ", expect: StreamId{Type: "users", Entity: ""}},
		{input: "users-peter   ", expect: StreamId{Type: "users", Entity: "peter"}},
		{input: "happy users-peter   ", expect: StreamId{Type: "happy users", Entity: "peter"}},
	}
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			// execute
			got := ParseStreamId(test.input)
			// verify
			assert.Equal(t, test.expect.Type, got.Type)
			assert.Equal(t, test.expect.Entity, got.Entity)
		})
	}
}

func TestStreamId_String(t *testing.T) {
	tests := []struct {
		input  StreamId
		expect string
	}{
		{input: StreamId{Type: "users", Entity: ""}, expect: "users"},
		{input: StreamId{Type: "users", Entity: "peter"}, expect: "users-peter"},
		{input: StreamId{Type: "users:commands", Entity: ""}, expect: "users:commands"},
		{input: StreamId{Type: "", Entity: "peter"}, expect: "-peter"},
		{input: StreamId{Type: "", Entity: ""}, expect: ""},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("%d-%s", i, test.input), func(t *testing.T) {
			// execute
			got := test.input.String()
			// verify
			assert.Equal(t, test.expect, got)
		})
	}
}
