
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
