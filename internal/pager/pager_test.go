package pager

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func callback(replies ...int) *recordingCallback {

	return &recordingCallback{
		replies: replies,
	}
}

type recordingCallback struct {
	calls   [][3]int64
	replies []int
}

func (cb *recordingCallback) Page(from, to, limit int64) (int, error) {
	cb.calls = append(cb.calls, [3]int64{from, to, limit})
	if len(cb.calls) <= len(cb.replies) {
		reply := cb.replies[len(cb.calls)-1]
		return reply, nil
	}
	return 0, nil
}

func TestFromTo(t *testing.T) {

	verify := func(t *testing.T, start, to int64, size int, cb *recordingCallback, expected [][3]int64) {
		t.Helper()
		err := FromTo(start, to, size, cb)
		assert.NoError(t, err)
		assert.Equal(t, expected, cb.calls)
	}

	t.Run("zero size", func(t *testing.T) {
		verify(t, 0, 10, 0, callback(), nil)
	})

	t.Run("one loop", func(t *testing.T) {
		verify(t, 0, 5, 5,
			callback(3),
			[][3]int64{{0, 5, 5}})
	})

	t.Run("two loops", func(t *testing.T) {
		verify(t, 0, 10, 5,
			callback(5, 3),
			[][3]int64{{0, 5, 5}, {5, 10, 5}})
	})

	t.Run("reach end", func(t *testing.T) {
		verify(t, 0, 5, 5,
			callback(5),
			[][3]int64{{0, 5, 5}})
	})

	t.Run("reach end second loop", func(t *testing.T) {
		verify(t, 0, 10, 5,
			callback(5, 5),
			[][3]int64{{0, 5, 5}, {5, 10, 5}})
	})

	t.Run("beyond end second loop", func(t *testing.T) {
		verify(t, 0, 8, 5,
			callback(5, 3),
			[][3]int64{{0, 5, 5}, {5, 8, 5}})
	})
}

func TestBySize(t *testing.T) {

	testNumberOfCalls := func(t *testing.T, start int64, size int, cb *recordingCallback, expected [][3]int64) {
		t.Helper()
		err := BySize(start, size, cb)
		// verify
		assert.NoError(t, err)
		assert.Equal(t, expected, cb.calls, "call mismatch")
	}

	t.Run("zero size", func(t *testing.T) {
		testNumberOfCalls(t, 0, 0, callback(), nil)
	})

	t.Run("half on first", func(t *testing.T) {
		testNumberOfCalls(t, 0, 5,
			callback(3),
			[][3]int64{{0, 5, 5}},
		)
	})

	t.Run("full on first", func(t *testing.T) {
		testNumberOfCalls(t, 0, 5,
			callback(5, 0),
			[][3]int64{{0, 5, 5}, {5, 10, 5}},
		)
	})

	t.Run("half on second", func(t *testing.T) {
		testNumberOfCalls(t, 0, 5,
			callback(5, 3),
			[][3]int64{{0, 5, 5}, {5, 10, 5}},
		)
	})

	t.Run("start further in", func(t *testing.T) {
		testNumberOfCalls(t, 10, 5,
			callback(5, 3),
			[][3]int64{{10, 15, 5}, {15, 20, 5}},
		)
	})

	t.Run("many calls", func(t *testing.T) {
		testNumberOfCalls(t, 0, 5,
			callback(5, 5, 5, 5, 0),
			[][3]int64{{0, 5, 5}, {5, 10, 5}, {10, 15, 5}, {15, 20, 5}, {20, 25, 5}},
		)
	})

	t.Run("break by error", func(t *testing.T) {
		err := fmt.Errorf("break")
		got := BySize(0, 5, Func(func(from, to, limit int64) (int, error) {
			return 0, err
		}))
		assert.Equal(t, err, got)
	})
}
