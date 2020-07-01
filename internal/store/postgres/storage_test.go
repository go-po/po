package postgres

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

const databaseUrl = "postgres://po:po@localhost:5431/po?sslmode=disable"

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
