package registry

import (
	"encoding/json"
	"fmt"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/stream"
	"log"
)

var DefaultRegistry = New()

type Named interface {
	Name() string
}

type MessageUnmarshaller func(b []byte) (interface{}, error)

type MessageType interface {
}

func New() *Registry {
	return &Registry{
		types: make(map[string]MessageUnmarshaller),
	}
}

type Registry struct {
	types map[string]MessageUnmarshaller
}

func Register(initializers ...MessageUnmarshaller) {
	DefaultRegistry.Register(initializers...)
}
func (reg *Registry) Register(initializers ...MessageUnmarshaller) {
	for _, initializer := range initializers {
		example, _ := initializer(nil)
		name := reg.LookupType(example)
		reg.types[name] = initializer
	}
}

func (reg *Registry) ToMessage(r record.Record) (stream.Message, error) {
	data, err := reg.Unmarshal(r.Type, r.Data)
	if err != nil {
		return stream.Message{}, err
	}
	return stream.Message{
		Number:      r.Number,
		Stream:      r.Stream,
		Data:        data,
		Type:        r.Type,
		GroupNumber: r.GroupNumber,
		Time:        r.Time,
	}, nil
}

func LookupType(msg interface{}) string {
	return DefaultRegistry.LookupType(msg)
}
func (reg *Registry) LookupType(msg interface{}) string {
	switch ex := msg.(type) {
	case Named:
		return ex.Name()
	default:
		return fmt.Sprintf("%T", ex)
	}
}

func Marshal(msg interface{}) ([]byte, error) {
	return DefaultRegistry.Marshal(msg)
}
func (reg *Registry) Marshal(msg interface{}) ([]byte, error) {
	return json.Marshal(msg)
}

func Unmarshal(typeName string, b []byte) (interface{}, error) {
	return DefaultRegistry.Unmarshal(typeName, b)
}
func (reg *Registry) Unmarshal(typeName string, b []byte) (interface{}, error) {
	unmarshal, found := reg.types[typeName]
	if !found {
		log.Printf("Known types")
		for t, _ := range reg.types {
			log.Printf("- %s", t)
		}
		return nil, fmt.Errorf("unknown message type: %s", typeName) // TODO error type
	}
	return unmarshal(b)
}
