#!/usr/bin/env bash
docker compose -f bin/docker-compose.yml down
docker container prune
docker rmi $(docker images -q) -f