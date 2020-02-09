package po

import "github.com/go-po/po/internal/record"

type Message struct {
	Id     int64       // place in the stream, starting at 1
	Stream string      // name of the stream this message belongs to
	Type   string      // name of the type of the message
	Data   interface{} // instance of the given Group
}

func ToMessage(registry Registry, r record.Record) (Message, error) {
	data, err := registry.Unmarshal(r.Type, r.Data)
	if err != nil {
		return Message{}, err
	}
	return Message{
		Id:     r.Number,
		Stream: r.Stream,
		Data:   data,
		Type:   r.Type,
	}, nil
}
