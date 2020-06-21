package postgres

import (
	"database/sql"
	"testing"

	"github.com/go-po/po/internal/store"
	"github.com/stretchr/testify/assert"
)

const databaseUrl = "postgres://po:po@localhost:5431/po?sslmode=disable"

func connect(t *testing.T) *PGStore {
	t.Helper()
	s, err := NewFromUrl(databaseUrl, store.StubObserver())
	if !assert.NoError(t, err, "connecting") {
		t.FailNow()
	}
	return s
}

func databaseConnection(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres", databaseUrl)
	if !assert.NoError(t, err, "database connection") {
		t.FailNow()
	}
	err = migrateDatabase(db)
	if !assert.NoError(t, err, "database migration") {
		t.FailNow()
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}
