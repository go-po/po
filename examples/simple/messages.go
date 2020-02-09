package simple

import (
	"encoding/json"
	"github.com/go-po/po"
)

type HelloMessage struct {
	Greeting string
}

func init() {
	po.RegisterMessages(
		func(b []byte) (interface{}, error) {
			msg := HelloMessage{}
			err := json.Unmarshal(b, &msg)
			return msg, err
		},
	)
}
