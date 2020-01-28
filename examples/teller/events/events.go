package events

import (
	"encoding/json"
	"github.com/go-po/po"
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
			err := json.Unmarshal(b, &msg)
			return msg, err
		},
		func(b []byte) (interface{}, error) {
			msg := SubtractedEvent{}
			err := json.Unmarshal(b, &msg)
			return msg, err
		},
		func(b []byte) (interface{}, error) {
			msg := DeclaredEvent{}
			err := json.Unmarshal(b, &msg)
			return msg, err
		},
	)
}
