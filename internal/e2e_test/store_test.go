package e2e_test

import (
	"context"
	"github.com/go-po/po"
	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/stream"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStorePostgres(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	StoreComplianceTest(t, pg())
}

func TestStoreInMemory(t *testing.T) {
	StoreComplianceTest(t, inmem())
}

func StoreComplianceTest(t *testing.T, builder StoreBuilder) {

	runStoreTestCase(t, builder, "store records", storeRecord)
	runStoreTestCase(t, builder, "assign group numbers", assignGroupNumber)
	runStoreTestCase(t, builder, "read records", readRecords)

}

type StoreTestCase func(t *testing.T, db po.Store)

func runStoreTestCase(t *testing.T, builder StoreBuilder, testName string, testCase StoreTestCase) {
	t.Run(testName, func(t *testing.T) {
		db, err := builder()
		if !assert.NoError(t, err, "failed building store") {
			t.FailNow()
		}
		testCase(t, db)
	})

}

func beginTx(t *testing.T, db po.Store) store.Tx {
	tx, err := db.Begin(context.Background())
	if !assert.NoError(t, err, "start tx") {
		t.FailNow()
	}
	t.Cleanup(func() {
		_ = tx.Commit()
	})
	return tx
}

func storeRecord(t *testing.T, db po.Store) {

	t.Run("in order", func(t *testing.T) {
		tx := beginTx(t, db)
		id := randStreamId("storeRecord", "")
		record, err := db.StoreRecord(tx, id, 1, "text/plain", []byte("storeRecord/in order"))
		if !assert.NoError(t, err, "store record") {
			t.FailNow()
		}
		assert.Equal(t, id, record.Stream)
		assert.Equal(t, "text/plain", record.ContentType)
		assert.Equal(t, "storeRecord/in order", string(record.Data))
	})

	t.Run("out of order", func(t *testing.T) {
		tx := beginTx(t, db)
		id := randStreamId("storeRecord", "")
		_, err := db.StoreRecord(tx, id, 404, "text/plain", []byte("storeRecord/out of order"))
		assert.Errorf(t, err, "out of order error")
	})

}

func readRecords(t *testing.T, db po.Store) {

	fixture := func(t *testing.T, id stream.Id, count int) {
		tx := beginTx(t, db)
		for i := 1; i <= count; i++ {
			_, err := db.StoreRecord(tx, id, int64(i), "text/plain", []byte("readRecords"))
			if !assert.NoError(t, err, "store record") {
				t.FailNow()
			}
		}
		err := tx.Commit()
		if !assert.NoError(t, err, "failed commit") {
			t.FailNow()
		}
		for i := 1; i <= count; i++ {
			_, err := db.AssignGroup(context.Background(), id, int64(i))
			if !assert.NoError(t, err, "assign group number") {
				t.FailNow()
			}
		}

	}

	t.Run("read known", func(t *testing.T) {
		// setup
		id := randStreamId("readRecords", "")
		fixture(t, id, 10)

		// execute
		records, err := db.ReadRecords(context.Background(), id, 0)

		// verify
		if !assert.NoError(t, err, "failed read") {
			t.FailNow()
		}
		assert.Equal(t, 10, len(records), "number of records read")
	})

	t.Run("read middle of entity stream", func(t *testing.T) {
		// setup
		id := randStreamId("readRecords", "aggregate")
		fixture(t, id, 10)

		// execute
		records, err := db.ReadRecords(context.Background(), id, 5)

		// verify
		if !assert.NoError(t, err, "failed read") {
			t.FailNow()
		}
		assert.Equal(t, 5, len(records), "number of records read")
		assert.Equal(t, int64(6), records[0].Number, "first number read")
	})

	t.Run("read middle of group stream", func(t *testing.T) {
		// setup
		groupStream := randStreamId("readMiddleGroup", "")
		streamA := groupStream
		streamA.Entity = "A"
		streamB := groupStream
		streamB.Entity = "B"
		fixture(t, streamA, 10)
		fixture(t, streamB, 10)

		// execute
		records, err := db.ReadRecords(context.Background(), groupStream, 10)

		// verify
		if !assert.NoError(t, err, "failed read") {
			t.FailNow()
		}
		if !assert.Equal(t, 10, len(records), "number of records read") {
			t.FailNow()
		}
		assert.Equal(t, int64(11), records[0].GroupNumber, "first number read")
	})

	t.Run("read unknown", func(t *testing.T) {
		// setup
		id := randStreamId("unknownStream", "")

		// execute
		records, err := db.ReadRecords(context.Background(), id, 0)

		// verify
		if !assert.NoError(t, err, "failed read") {
			t.FailNow()
		}
		assert.Equal(t, 0, len(records), "number of records read")
	})

}

func assignGroupNumber(t *testing.T, db po.Store) {
	fixture := func(t *testing.T, id stream.Id, count int) {
		tx := beginTx(t, db)
		for i := 1; i <= count; i++ {
			_, err := db.StoreRecord(tx, id, int64(i), "text/plain", []byte("assignGroupNumber"))
			if !assert.NoError(t, err, "store record") {
				t.FailNow()
			}
		}
		err := tx.Commit()
		if !assert.NoError(t, err, "failed commit") {
			t.FailNow()
		}
	}

	t.Run("append entity stream", func(t *testing.T) {
		// setup
		id := randStreamId("assignEntityStream", "entity")
		fixture(t, id, 1)

		// execute
		record, err := db.AssignGroup(context.Background(), id, 1)

		// verify
		if !assert.NoError(t, err, "failed assign") {
			t.FailNow()
		}
		assert.Equal(t, int64(1), record.Number)
		assert.Equal(t, int64(1), record.GroupNumber)

	})

	t.Run("append the group stream", func(t *testing.T) {
		// setup
		id := randStreamId("assignGroupStream", "")
		fixture(t, id, 1)

		// execute
		record, err := db.AssignGroup(context.Background(), id, 1)

		// verify
		if !assert.NoError(t, err, "failed assign") {
			t.FailNow()
		}
		assert.Equal(t, int64(1), record.Number)
		assert.Equal(t, int64(1), record.GroupNumber)
	})

	t.Run("append two entity streams in same group", func(t *testing.T) {
		// setup
		group := randStreamId("assignGroupMultiStream", "")
		streamA := group
		streamA.Entity = "A"
		streamB := group
		streamB.Entity = "B"

		fixture(t, streamA, 1)
		fixture(t, streamB, 1)

		// execute
		a, err := db.AssignGroup(context.Background(), streamA, 1)
		if !assert.NoError(t, err, "failed assign") {
			t.FailNow()
		}
		b, err := db.AssignGroup(context.Background(), streamB, 1)
		if !assert.NoError(t, err, "failed assign") {
			t.FailNow()
		}

		// verify
		assert.Equal(t, int64(1), a.Number)
		assert.Equal(t, int64(1), a.GroupNumber)

		assert.Equal(t, int64(1), b.Number)
		assert.Equal(t, int64(2), b.GroupNumber)
	})

	t.Run("append group and entity stream", func(t *testing.T) {
		// setup
		groupStream := randStreamId("assignGroupCombiStream", "")
		entityStream := groupStream
		entityStream.Entity = "entity"

		fixture(t, entityStream, 1)
		fixture(t, groupStream, 1)

		// execute
		entity, err := db.AssignGroup(context.Background(), entityStream, 1)
		if !assert.NoError(t, err, "failed assign") {
			t.FailNow()
		}
		group, err := db.AssignGroup(context.Background(), groupStream, 1)
		if !assert.NoError(t, err, "failed assign") {
			t.FailNow()
		}

		// verify
		assert.Equal(t, int64(1), entity.Number, "entityStreams number")
		assert.Equal(t, int64(1), entity.GroupNumber, "entityStreams group number")

		assert.Equal(t, int64(1), group.Number, "groupStreams number")
		assert.Equal(t, int64(2), group.GroupNumber, "groupStreams group number")
	})

}
