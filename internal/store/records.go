package store

type Record struct {
	Id     int64
	Stream string
	Data   []byte
	Type   string
}
