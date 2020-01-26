package registry

import (
	"encoding/json"
	"fmt"
	"log"
)

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

var DefaultRegistry = New()
