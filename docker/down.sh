#!/bin/bash

docker stop hadoop3.3-cluster
docker rm hadoop3.3-cluster
docker build -t kinghtdom/hadoop3.3-cluster .
docker network create --driver=bridge hadoop

docker run -it --name hadoop3.3-cluster \
--network hadoop \
-h h01 \
--name hadoop01 \
-p 9870:9870 \
-p 8088:8088 \
-v /Users/edgar/Documents/www/BigData:/www/ \
-d hadoop3.3-cluster

docker run -it --name hadoop3.3-cluster \
--network hadoop \
-h h02 \
--name hadoop02 \
-v /Users/edgar/Documents/www/BigData:/www/ \
-d hadoop3.3-cluster

docker run -it --name hadoop3.3-cluster \
--network hadoop \
-h h03 \
--name hadoop03 \
-v /Users/edgar/Documents/www/BigData:/www/ \
-d hadoop3.3-cluster