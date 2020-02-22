
db-reset: gen
	echo "drop schema public cascade" 			| docker exec -i po_pg psql -U po po
	echo "create schema if not exists public" 	| docker exec -i po_pg psql -U po po

test:
	go test ./... -count 1 -race

test-all:
	go test ./... -count 1 -tags integration

gen:
	go generate ./...

up:
	docker-compose up -d

down:
	docker-compose down

plantuml:
	./scripts/plantuml.sh
