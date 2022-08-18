# even-driven-ex

How to use even driven

## Docker compose cockroach

[here](https://kb.objectrocket.com/cockroachdb/docker-compose-and-cockroachdb-1151)

## Init base db

docker-compose up --build
sudo docker exec -it a83b9127bcb8 ./cockroach init --insecure --host=localhost:26257
sudo docker exec -it 19c4d418b1e4 ./cockroach sql --insecure

## Test nats

docker run --network nats --rm -it synadia/nats-box
nats sub -s nats://nats:4222 hello &
nats pub -s "nats://nats-1:4222" hello first
nats pub -s "nats://nats-2:4222" hello second
