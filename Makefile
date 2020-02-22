
gen:
	go generate ./...

up:
	docker-compose up -d

down:
	docker-compose down

plantuml:
	./scripts/plantuml.sh
