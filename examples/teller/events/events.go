package events

import (
	"encoding/json"
	"github.com/kyuff/po"
)

type AddedEvent struct {
	Value int64
}

type SubtractedEvent struct {
	Value int64
}

type DeclaredEvent struct {
	Name string
}

func init() {
	po.RegisterMessages(
		func(b []byte) (interface{}, error) {
			msg := AddedEvent{}
			return msg, json.Unmarshal(b, &msg)
		},
		func(b []byte) (interface{}, error) {
			msg := SubtractedEvent{}
			return msg, json.Unmarshal(b, &msg)
		},
		func(b []byte) (interface{}, error) {
			msg := DeclaredEvent{}
			return msg, json.Unmarshal(b, &msg)
		},
	)
}
