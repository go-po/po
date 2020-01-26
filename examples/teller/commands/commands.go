package commands

import (
	"encoding/json"
	"github.com/kyuff/po"
)

// Add the number to the counter
type Add struct {
	Number int64
}

// Subtract the number from the counter
type Sub struct {
	Number int64
}

type DeclareVar struct {
	Name string
}

func init() {
	po.RegisterMessages(
		func(b []byte) (interface{}, error) {
			msg := Add{}
			return msg, json.Unmarshal(b, &msg)
		},
		func(b []byte) (interface{}, error) {
			msg := Sub{}
			return msg, json.Unmarshal(b, &msg)
		},
		func(b []byte) (interface{}, error) {
			msg := DeclareVar{}
			return msg, json.Unmarshal(b, &msg)
		},
	)
}
