package commands

import (
	"encoding/json"
	"github.com/kyuff/po"
)

// AddCommand the number to the counter
type AddCommand struct {
	Name   string
	Number int64
}

// Subtract the number from the counter
type SubCommand struct {
	Name   string
	Number int64
}

type DeclareCommand struct {
	Name string
}

func init() {
	po.RegisterMessages(
		func(b []byte) (interface{}, error) {
			msg := AddCommand{}
			return msg, json.Unmarshal(b, &msg)
		},
		func(b []byte) (interface{}, error) {
			msg := SubCommand{}
			return msg, json.Unmarshal(b, &msg)
		},
		func(b []byte) (interface{}, error) {
			msg := DeclareCommand{}
			return msg, json.Unmarshal(b, &msg)
		},
	)
}
