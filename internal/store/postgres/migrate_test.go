package postgres

import (
	"database/sql"
	"testing"

	_ "github.com/lib/pq" // load driver
	"github.com/stretchr/testify/assert"
)

func TestMigrateDatabase(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// setup
	db, err := sql.Open("postgres", databaseUrl)
	assert.NoError(t, err, "connecting")

	// execute
	err = migrateDatabase(db)

	// verify
	assert.NoError(t, err, "migration failed")
}
