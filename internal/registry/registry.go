package registry

import (
	"encoding/json"
	"fmt"
	"log"
	"mime"

	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/streams"
)

var DefaultRegistry = New()

const paramNameType = "type"

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

func (reg *Registry) ToMessage(r record.Record) (streams.Message, error) {
	_, params, err := mime.ParseMediaType(r.ContentType)
	if err != nil {
		return streams.Message{}, err
	}

	typeName, ok := params[paramNameType]
	if !ok {
		return streams.Message{}, fmt.Errorf("registry: field '%s' not in '%s'", paramNameType, r.ContentType)
	}
	data, err := reg.Unmarshal(typeName, r.Data)
	if err != nil {
		return streams.Message{}, err
	}
	return streams.Message{
		Number:      r.Number,
		Stream:      r.Stream,
		Data:        data,
		Type:        typeName,
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
		paramNameType: getType(msg),
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
		for t := range reg.types {
			log.Printf("%s - %s", typeName, t)
		}
		return nil, fmt.Errorf("unknown message type: %s", typeName) // TODO error type
	}
	return unmarshal(b)
}
