package postgres

import (
	"context"
	"errors"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/streams"
	"github.com/stretchr/testify/assert"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func streamId(entity string) streams.Id {
	prefix := strconv.FormatInt(rand.Int63(), 10)
	if len(entity) > 0 {
		return streams.ParseId("%s-%s", prefix, entity)
	}
	return streams.ParseId(prefix)
}

func data(c int) []record.Data {
	var result []record.Data
	for i := 0; i < c; i++ {
		result = append(result, record.Data{
			ContentType: "application/json",
			Data:        []byte("{}"),
		})
	}
	return result
}

func TestStorage_WriteRecords(t *testing.T) {
	// setup
	conn := databaseConnection(t)
	ctx := context.Background()

	t.Run("single record", func(t *testing.T) {
		// setup
		id := streamId("single")
		// execute
		got, err := writeRecords(ctx, conn, id, -1, data(1)...)
		// verify
		assert.NoError(t, err)
		if assert.Equal(t, 1, len(got)) {
			assert.Equal(t, id.String(), got[0].Stream.String())
			assert.Equal(t, 0, int(got[0].Number))
		}
	})

	t.Run("multiple records", func(t *testing.T) {
		// setup
		id := streamId("multiple")
		// execute
		got, err := writeRecords(ctx, conn, id, -1, data(4)...)
		// verify
		assert.NoError(t, err)
		if assert.Equal(t, 4, len(got)) {
			assert.Equal(t, id.String(), got[0].Stream.String())
			assert.Equal(t, 0, int(got[0].Number))

			assert.Equal(t, id.String(), got[3].Stream.String())
			assert.Equal(t, 3, int(got[3].Number))
		}
	})

	t.Run("conflict", func(t *testing.T) {
		// setup
		id := streamId("conflict")
		_, err := writeRecords(ctx, conn, id, -1, data(5)...)
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		// execute
		_, err = writeRecords(ctx, conn, id, 3, data(2)...)
		// verify
		if assert.Error(t, err) {
			conflict := store.WriteConflictError{}
			if errors.As(err, &conflict) {
				assert.Equal(t, id.String(), conflict.StreamId.String())
				assert.Equal(t, 4, int(conflict.Position))
			} else {
				t.Logf("unexpected error type: %T", err)
				t.FailNow()
			}
		}
	})
}
