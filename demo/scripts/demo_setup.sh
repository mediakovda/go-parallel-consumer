#!/bin/bash
cd "${0%/*}"

set -a
. .env
set +a

set -x

docker-compose up -d

docker exec broker \
kafka-topics \
    --bootstrap-server $broker_internal \
    --create \
    --if-not-exists \
    --topic $topic \
    --partitions $partitions

go run ../main.go -produce -bootstrap $broker -topic $topic -n 1000000

set +x
