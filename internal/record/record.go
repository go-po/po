package record

import (
	"github.com/go-po/po/stream"
	"time"
)

// Internal data structure to pass between
// the components that make up Po
type Record struct {
	Number      int64     // strictly sequential number for all messages in a specific stream
	Stream      stream.Id // identifier of a stream, see id.go
	Data        []byte    // raw data for the message
	Type        string    // message type, used to marshal tye Data correct
	GroupNumber int64     // strictly sequential number for all messages in a group
	Time        time.Time // when this message was first recorded
}
