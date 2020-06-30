package pager

import "math"

type Pager interface {
	// from and to are numbers in the ordered list that is paged
	// limit is how many can be read per page
	// the pager returns how many where read
	// returning a number less than the limit will
	// result in the pagination to stop
	Page(from, to, limit int64) (int, error)
}

type Func func(from, to, limit int64) (int, error)

func (fn Func) Page(from, to, limit int64) (int, error) {
	return fn(from, to, limit)
}

func BySize(start int64, size int, cb Pager) error {
	if size == 0 {
		return nil
	}
	done, err := cb.Page(start, start+int64(size), int64(size))
	if err != nil {
		return err
	}
	if done < size || done == 0 {
		return nil
	}
	return BySize(start+int64(done), size, cb)
}

func FromTo(start int64, to int64, size int, cb Pager) error {
	if size == 0 || to <= start {
		return nil
	}

	end := start + int64(size)
	if end > to {
		end = to
	}

	done, err := cb.Page(start, end, int64(size))
	if err != nil {
		return err
	}

	if done < size || done == 0 {
		return nil
	}

	return FromTo(start+int64(done), to, size, cb)
}

func ToMax(start int64, size int, cb Pager) error {
	return FromTo(start, math.MaxInt64, size, cb)
}
