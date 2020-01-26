package events

import (
	"encoding/json"
	"github.com/kyuff/po"
)

type Added struct {
	Value int64
}

type Subtracted struct {
	Value int64
}

type Declared struct {
	Name string
}

func init() {
	po.RegisterMessages(
		func() po.MessageType { return Added{} },
		func() po.MessageType { return Subtracted{} },
		func() po.MessageType { return Declared{} },
	)
}

func init() {
	po.RegisterMessages(
		func(b []byte) (interface{}, error) {
			msg := Added{}
			return msg, json.Unmarshal(b, &msg)
		},
		func(b []byte) (interface{}, error) {
			msg := Subtracted{}
			return msg, json.Unmarshal(b, &msg)
		},
		func(b []byte) (interface{}, error) {
			msg := Declared{}
			return msg, json.Unmarshal(b, &msg)
		},
	)
}
