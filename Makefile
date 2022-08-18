SHELL := /bin/bash

nats:
	docker-compose -f nats-cluster.yml up

nats-stop:
	docker-compose -f nats-cluster.yml down && docker-compose -f nats-cluster.yml stop

.PHONY: test