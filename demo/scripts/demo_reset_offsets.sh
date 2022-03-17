#!/bin/bash
cd "${0%/*}"

set -a
. .env
set +a

set -x

docker exec broker \
kafka-consumer-groups \
    --bootstrap-server $broker_internal \
    --topic $topic \
    --group $groupid \
    --reset-offsets --to-earliest \
    --execute

set +x
