package record

import (
	"time"

	"github.com/go-po/po/streams"
)

// Internal data structure to pass between
// the components that make up Po
type Record struct {
	Number      int64      // strictly sequential number for all messages in a specific stream
	Stream      streams.Id // identifier of a stream, see id.go
	Data        []byte     // raw data for the message
	Group       string     // message type, used to marshal tye Data correct
	ContentType string     // type of the data
	GroupNumber int64      // strictly sequential number for all messages in a group
	Time        time.Time  // when this message was first recorded
}

type Snapshot struct {
	Data        []byte
	Position    int64
	ContentType string
}

type Data struct {
	ContentType string
	Data        []byte
}
