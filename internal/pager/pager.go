package pager

type Pager interface {
	Page(from, to int64) (int, error)
}

type Func func(from, to int64) (int, error)

func (fn Func) Page(from, to int64) (int, error) {
	return fn(from, to)
}

func BySize(start int64, size int, cb Pager) error {
	if size == 0 {
		return nil
	}
	done, err := cb.Page(start, start+int64(size))
	if err != nil {
		return err
	}
	if done < size || done == 0 {
		return nil
	}
	return BySize(start+int64(done), size, cb)
}
