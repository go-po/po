package streams

import (
	"time"
)

type Message struct {
	Number        int64       // place in the stream, starting at 1
	GlobalNumber  int64       // Ordering across all messages in this Event Source
	Stream        Id          // name of the stream this message belongs to
	Type          string      // name of the type of the message
	Data          interface{} // instance of the given Group
	CorrelationId string      // Application generated id to correlate messages
	Time          time.Time   // time the message was first recorded
}
