package po

import (
	"fmt"
	"log"
)

type MessageType interface {
}

var messageTypes map[string]MessageUnmarshaller

func init() {
	messageTypes = make(map[string]MessageUnmarshaller)
}

type Named interface {
	Name() string
}

type MessageUnmarshaller func(b []byte) (interface{}, error)

func RegisterMessages(initializers ...MessageUnmarshaller) {
	for _, initializer := range initializers {
		example, _ := initializer(nil)
		name := lookupTypeName(example)
		messageTypes[name] = initializer
	}
}

func lookupTypeName(msg interface{}) string {
	switch ex := msg.(type) {
	case Named:
		return ex.Name()
	default:
		return fmt.Sprintf("%T", ex)
	}
}

func LookupData(typeName string, b []byte) (interface{}, error) {
	unmarshal, found := messageTypes[typeName]
	if !found {
		log.Printf("Known types")
		for t, _ := range messageTypes {
			log.Printf("- %s", t)
		}
		return nil, fmt.Errorf("unknown message type: %s", typeName) // TODO error type
	}

	return unmarshal(b)
}
