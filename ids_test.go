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
		{input: "users", expect: StreamId{Group: "users", Entity: ""}},
		{input: "users:commands", expect: StreamId{Group: "users:commands", Entity: ""}},
		{input: "users-peter", expect: StreamId{Group: "users", Entity: "peter"}},
		{input: "users-peter-peter", expect: StreamId{Group: "users", Entity: "peter-peter"}},
		{input: "users-", expect: StreamId{Group: "users", Entity: ""}},
		{input: "-users", expect: StreamId{Group: "", Entity: "users"}},
		{input: "", expect: StreamId{Group: "", Entity: ""}},
		{input: "  users", expect: StreamId{Group: "users", Entity: ""}},
		{input: "users   ", expect: StreamId{Group: "users", Entity: ""}},
		{input: "users-peter   ", expect: StreamId{Group: "users", Entity: "peter"}},
		{input: "happy users-peter   ", expect: StreamId{Group: "happy users", Entity: "peter"}},
	}
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			// execute
			got := ParseStreamId(test.input)
			// verify
			assert.Equal(t, test.expect.Group, got.Group)
			assert.Equal(t, test.expect.Entity, got.Entity)
		})
	}
}

func TestStreamId_String(t *testing.T) {
	tests := []struct {
		input  StreamId
		expect string
	}{
		{input: StreamId{Group: "users", Entity: ""}, expect: "users"},
		{input: StreamId{Group: "users", Entity: "peter"}, expect: "users-peter"},
		{input: StreamId{Group: "users:commands", Entity: ""}, expect: "users:commands"},
		{input: StreamId{Group: "", Entity: "peter"}, expect: "-peter"},
		{input: StreamId{Group: "", Entity: ""}, expect: ""},
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
