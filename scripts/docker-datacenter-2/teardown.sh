#!/bin/sh

docker-compose -p kaproxy2 down
docker-compose -p kaproxy2 rm
docker volume prune -f
