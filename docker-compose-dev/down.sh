#!/bin/bash

docker rm $(docker ps -a -q)

cd './sawtooth'
    docker-compose -f ./docker-compose.yaml down
cd -