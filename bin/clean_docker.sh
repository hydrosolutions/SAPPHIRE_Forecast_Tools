#!/bin/bash
docker compose -f bin/docker-compose.yml down
docker container prune -f
docker rmi $(docker images -q) -f