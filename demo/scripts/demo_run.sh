#!/bin/bash
cd "${0%/*}"

set -a
. .env
set +a

set -x

go run ../main.go -consume -bootstrap $broker -topic $topic -max 150000

set +x
