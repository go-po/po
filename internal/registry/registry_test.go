package registry

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRegistry_RoundtripMarshal(t *testing.T) {
	// setup
	type A struct {
		A int
	}

	reg := New()
	reg.Register(
		func(b []byte) (interface{}, error) {
			msg := A{}
			err := json.Unmarshal(b, &msg)
			return msg, err
		},
	)
	a := A{
		A: 42,
	}
	typeName := reg.LookupType(a)

	// execute round trip
	b, err := reg.Marshal(a)

	assert.NoError(t, err)
	assert.Equal(t, []byte(`{"A":42}`), b)

	got, err := reg.Unmarshal(typeName, b)
	assert.NoError(t, err)

	gotA, ok := got.(A)
	if assert.True(t, ok) {
		assert.Equal(t, 42, gotA.A)
	}
}
