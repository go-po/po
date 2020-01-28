package commands

import (
	"encoding/json"
	"github.com/go-po/po"
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
			msg := DeclareCommand{}
			err := json.Unmarshal(b, &msg)
			return msg, err
		},
		func(b []byte) (interface{}, error) {
			msg := AddCommand{}
			err := json.Unmarshal(b, &msg)
			return msg, err
		},
		func(b []byte) (interface{}, error) {
			msg := SubCommand{}
			err := json.Unmarshal(b, &msg)
			return msg, err
		},
	)
}
