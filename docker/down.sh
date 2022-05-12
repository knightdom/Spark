#!/bin/bash

docker stop hadoop3.3-cluster
docker rm hadoop3.3-cluster
docker build -t kinghtdom/hadoop3.3-cluster .
docker-compose up
