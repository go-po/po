package stream

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseStreamId(t *testing.T) {
	tests := []struct {
		input  string
		expect Id
	}{
		{input: "users", expect: Id{Group: "users", Entity: ""}},
		{input: "users:commands", expect: Id{Group: "users:commands", Entity: ""}},
		{input: "users-peter", expect: Id{Group: "users", Entity: "peter"}},
		{input: "users-peter-peter", expect: Id{Group: "users", Entity: "peter-peter"}},
		{input: "users-", expect: Id{Group: "users", Entity: ""}},
		{input: "-users", expect: Id{Group: "", Entity: "users"}},
		{input: "", expect: Id{Group: "", Entity: ""}},
		{input: "  users", expect: Id{Group: "users", Entity: ""}},
		{input: "users   ", expect: Id{Group: "users", Entity: ""}},
		{input: "users-peter   ", expect: Id{Group: "users", Entity: "peter"}},
		{input: "happy users-peter   ", expect: Id{Group: "happy users", Entity: "peter"}},
	}
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			// execute
			got := ParseId(test.input)
			// verify
			assert.Equal(t, test.expect.Group, got.Group)
			assert.Equal(t, test.expect.Entity, got.Entity)
		})
	}
}

func TestStreamId_String(t *testing.T) {
	tests := []struct {
		input  Id
		expect string
	}{
		{input: Id{Group: "users", Entity: ""}, expect: "users"},
		{input: Id{Group: "users", Entity: "peter"}, expect: "users-peter"},
		{input: Id{Group: "users:commands", Entity: ""}, expect: "users:commands"},
		{input: Id{Group: "", Entity: "peter"}, expect: "-peter"},
		{input: Id{Group: "", Entity: ""}, expect: ""},
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
