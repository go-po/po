package po

type Message struct {
	Id     int64       // place in the stream, starting at 1
	Stream string      // name of the stream this message belongs to
	Type   string      // name of the type of the message
	Data   interface{} // instance of the given Type
}
