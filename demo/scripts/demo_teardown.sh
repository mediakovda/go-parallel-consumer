#!/bin/bash
cd "${0%/*}"

set -a
. .env
set +a

set +x

# docker exec broker \
# kafka-topics \
#     --bootstrap-server $broker_internal \
#     --delete \
#     --if-exists \
#     --topic $topic

docker-compose down

set -x
