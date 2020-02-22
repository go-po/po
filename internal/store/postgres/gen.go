package postgres

//go:generate go run github.com/kyleconroy/sqlc/cmd/sqlc generate
//go:generate go run github.com/jteeuwen/go-bindata/go-bindata -prefix schema -o ./generated/db/migrations.go -ignore .go -pkg db schema
