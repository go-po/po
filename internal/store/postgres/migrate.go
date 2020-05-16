package postgres

import (
	"database/sql"
	"fmt"

	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/lib/pq" // required

	"github.com/go-po/po/internal/store/postgres/generated/db"
	"github.com/golang-migrate/migrate/v4"
	bindata "github.com/golang-migrate/migrate/v4/source/go_bindata"
)

func migrateDatabase(conn *sql.DB) error {

	d, err := postgres.WithInstance(conn, &postgres.Config{
		MigrationsTable: "po_migrations",
	})
	if err != nil {
		return fmt.Errorf("driver: %w", err)
	}

	resource := bindata.Resource(db.AssetNames(),
		func(name string) ([]byte, error) {
			return db.Asset(name)
		})
	data, err := bindata.WithInstance(resource)
	if err != nil {
		return fmt.Errorf("bindata: %w", err)
	}

	migrates, err := migrate.NewWithInstance("migrations", data, "po", d)
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	// run migrations and handle the errors above of course
	err = migrates.Up()

	switch err {
	case migrate.ErrNoChange:
		return nil
	case nil:
		return nil
	default:
		return fmt.Errorf("up: %w", err)
	}
}
