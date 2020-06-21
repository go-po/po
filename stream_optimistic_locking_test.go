package po

import (
	"encoding/json"

	"github.com/go-po/po/internal/registry"
)

type Msg struct {
	Name string
}

var testRegistry = registry.New()

func init() {
	testRegistry.Register(func(b []byte) (interface{}, error) {
		msg := Msg{}
		err := json.Unmarshal(b, &msg)
		return msg, err
	})
}
