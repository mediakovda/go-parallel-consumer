#!/bin/bash
cd "${0%/*}"

set -a
. .env
set +a

set -x

docker exec broker \
kafka-topics \
    --bootstrap-server $broker_internal \
    --describe \
    --topic $topic

docker exec broker \
kafka-get-offsets \
    --bootstrap-server $broker_internal \
    --topic $topic

docker exec broker \
kafka-consumer-groups \
    --bootstrap-server $broker_internal \
    --group $groupid \
    --describe

set +x
