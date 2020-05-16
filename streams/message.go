package streams

import (
	"time"
)

type Message struct {
	Number      int64       // place in the stream, starting at 1
	Stream      Id          // name of the stream this message belongs to
	Type        string      // name of the type of the message
	Data        interface{} // instance of the given Group
	GroupNumber int64       // Ordering within the group
	Time        time.Time   // time the message was first recorded
}
