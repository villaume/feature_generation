#!/usr/bin/env bash
docker-compose -f ./docker/docker-compose.yml stop
docker-compose -f ./docker/docker-compose.yml rm -v -f
