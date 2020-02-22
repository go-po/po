package postgres

import (
	"database/sql"
	"fmt"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/lib/pq" // required

	"github.com/go-po/po/internal/store/postgres/migrations"
	"github.com/golang-migrate/migrate/v4"
	bindata "github.com/golang-migrate/migrate/v4/source/go_bindata"
)

//go:generate go run github.com/jteeuwen/go-bindata/go-bindata -prefix migrations -o ./migrations/migrations.go -ignore .go -pkg migrations migrations

func migrateDatabase(db *sql.DB) error {

	d, err := postgres.WithInstance(db, &postgres.Config{
		MigrationsTable: "po_migrations",
	})
	if err != nil {
		return fmt.Errorf("driver: %w", err)
	}

	resource := bindata.Resource(migrations.AssetNames(),
		func(name string) ([]byte, error) {
			return migrations.Asset(name)
		})
	data, err := bindata.WithInstance(resource)
	if err != nil {
		return fmt.Errorf("bindata: %w", err)
	}

	migrator, err := migrate.NewWithInstance("migrations", data, "po", d)
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	// run migrations and handle the errors above of course
	err = migrator.Up()

	switch err {
	case migrate.ErrNoChange:
		return nil
	case nil:
		return nil
	default:
		return fmt.Errorf("up: %w", err)
	}
}
