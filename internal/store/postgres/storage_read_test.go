package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStorage_ReadRecords(t *testing.T) {
	// setup
	conn := databaseConnection(t)
	ctx := context.Background()

	t.Run("empty stream", func(t *testing.T) {
		// setup
		id := streamId("entity")
		// execute
		records, err := readRecords(ctx, conn, id, -1)
		// verify
		assert.NoError(t, err)
		assert.Empty(t, records)
	})

	t.Run("empty group", func(t *testing.T) {
		// setup
		id := streamId("")
		// execute
		records, err := readRecords(ctx, conn, id, -1)
		// verify
		assert.NoError(t, err)
		assert.Empty(t, records)
	})

	t.Run("mid stream", func(t *testing.T) {
		// setup
		id := streamId("entity")
		_, err := writeRecords(ctx, conn, id, -1, data(10)...)
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		// execute
		records, err := readRecords(ctx, conn, id, 5)
		// verify
		assert.NoError(t, err)
		if assert.Equal(t, 4, len(records)) {
			assert.Equal(t, 9, int(records[3].Number))
			assert.Equal(t, id.String(), records[3].Stream.String())
		}
	})

	t.Run("mid group", func(t *testing.T) {
		// setup
		group := streamId("")
		id1 := group.WithEntity("entity-1")
		id2 := group.WithEntity("entity-2")
		var i int64
		var middle int64
		for i = 0; i < 5; i++ {
			r, err := writeRecords(ctx, conn, id1, i, data(1)...)
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			if i == 2 {
				// middle
				middle = r[0].GroupNumber
			}
			_, err = writeRecords(ctx, conn, id2, i, data(1)...)
			if !assert.NoError(t, err) {
				t.FailNow()
			}
		}
		// execute
		records, err := readRecords(ctx, conn, group, middle)
		// verify
		assert.NoError(t, err)
		if assert.Equal(t, 5, len(records)) {
			assert.Equal(t, 5, int(records[3].Number))
		}
	})
}
