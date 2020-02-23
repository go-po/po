package registry

import (
	"encoding/json"
	"fmt"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/stream"
	"log"
	"mime"
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
		name := getType(example)
		reg.types[name] = initializer
	}
}

func (reg *Registry) ToMessage(r record.Record) (stream.Message, error) {
	data, err := reg.Unmarshal(r.Group, r.Data)
	if err != nil {
		return stream.Message{}, err
	}
	return stream.Message{
		Number:      r.Number,
		Stream:      r.Stream,
		Data:        data,
		Type:        r.Group,
		GroupNumber: r.GroupNumber,
		Time:        r.Time,
	}, nil
}

func getType(msg interface{}) string {
	switch ex := msg.(type) {
	case Named:
		return ex.Name()
	default:
		return fmt.Sprintf("%T", ex)
	}
}

func (reg *Registry) lookupContentType(msg interface{}) string {
	return mime.FormatMediaType("application/json", map[string]string{
		"type": getType(msg),
	})
}

func (reg *Registry) Marshal(msg interface{}) ([]byte, string, error) {
	b, err := json.Marshal(msg)
	return b, reg.lookupContentType(msg), err
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
