package postgres

import (
	"context"
	"testing"

	"github.com/go-po/po/internal/record"
	"github.com/stretchr/testify/assert"
)

func TestStorage_Snapshot(t *testing.T) {
	// setup
	conn := databaseConnection(t)
	ctx := context.Background()

	t.Run("read nil snapshot", func(t *testing.T) {
		// setup
		id := streamId("nil")
		// execute
		snapshot, err := readSnapshot(ctx, conn, id, "not there")
		// verify
		assert.NoError(t, err)
		assert.Equal(t, -1, int(snapshot.Position))
	})

	t.Run("first update", func(t *testing.T) {
		// setup
		id := streamId("first")
		// execute
		err := updateSnapshot(ctx, conn, id, "first snapshot", record.Snapshot{
			Data:        []byte(`{}`),
			Position:    4,
			ContentType: "application/json",
		})
		// verify
		assert.NoError(t, err)
	})

	t.Run("write/read", func(t *testing.T) {
		// setup
		id := streamId("write/read")
		err := updateSnapshot(ctx, conn, id, "write/read", record.Snapshot{
			Data:        []byte(`{ "Key" : 5 }`),
			Position:    7,
			ContentType: "application/json",
		})
		if !assert.NoError(t, err, "write") {
			t.FailNow()
		}

		// execute
		snapshot, err := readSnapshot(ctx, conn, id, "write/read")

		// verify
		assert.NoError(t, err)
		assert.Equal(t, 7, int(snapshot.Position))
		assert.Equal(t, []byte(`{ "Key" : 5 }`), snapshot.Data)
	})

	t.Run("write/write/read", func(t *testing.T) {
		// setup
		id := streamId("write/write/read")
		err := updateSnapshot(ctx, conn, id, "write/write/read", record.Snapshot{
			Data:        []byte(`{ "Key" : 1 }`),
			Position:    4,
			ContentType: "application/json",
		})
		if !assert.NoError(t, err, "write") {
			t.FailNow()
		}
		err = updateSnapshot(ctx, conn, id, "write/write/read", record.Snapshot{
			Data:        []byte(`{ "Key" : 2 }`),
			Position:    5,
			ContentType: "application/json",
		})
		if !assert.NoError(t, err, "write") {
			t.FailNow()
		}

		// execute
		snapshot, err := readSnapshot(ctx, conn, id, "write/write/read")

		// verify
		assert.NoError(t, err)
		assert.Equal(t, 5, int(snapshot.Position))
		assert.Equal(t, []byte(`{ "Key" : 2 }`), snapshot.Data)
	})

	t.Run("write/delete/read", func(t *testing.T) {
		// setup
		id := streamId("write/read")
		err := updateSnapshot(ctx, conn, id, "write/delete/read", record.Snapshot{
			Data:        []byte(`{ "Key" : 9 }`),
			Position:    9,
			ContentType: "application/json",
		})
		if !assert.NoError(t, err, "write") {
			t.FailNow()
		}

		// execute
		err = deleteSnapshot(ctx, conn, id, "write/delete/read")

		// verify
		if !assert.NoError(t, err) {
			snapshot, err := readSnapshot(ctx, conn, id, "write/delete/read")
			assert.NoError(t, err)
			assert.Equal(t, -1, int(snapshot.Position))
			assert.Equal(t, []byte(`{}`), snapshot.Data)
		}
	})

}
