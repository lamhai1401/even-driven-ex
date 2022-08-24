SHELL := /bin/bash

proto:
	protoc ./evenstream/eventstore/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.
db:
	docker-compose -f postgres.yml up

db-stop:
	docker-compose -f postgres.yml down && docker-compose -f postgres.yml stop

nats:
	docker-compose -f nats-cluster.yml up

nats-stop:
	docker-compose -f nats-cluster.yml down && docker-compose -f nats-cluster.yml stop

.PHONY: test