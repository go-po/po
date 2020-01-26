package record

// Internal data structure to pass between
// the components that make up Po
type Record struct {
	Id     int64
	Stream string
	Data   []byte
	Type   string
}
