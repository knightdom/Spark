#!/bin/bash

docker stop hadoop01 hadoop02 hadoop03
docker rm hadoop01 hadoop02 hadoop03
docker build -t kinghtdom/hadoop3.3-cluster .
docker network create --driver=bridge --subnet 172.18.0.0/24 hadoop

docker run -it --name hadoop01 \
--network hadoop \
--ip 172.18.0.2 \
-h node1.kinghtdom.com \
-p 9870:9870 \
-p 8088:8088 \
-v /Users/edgar/Documents/www/BigData:/www/ \
-d kinghtdom/hadoop3.3-cluster

docker run -it --name hadoop02 \
--network hadoop \
--ip 172.18.0.3 \
-h node2.kinghtdom.com \
-v /Users/edgar/Documents/www/BigData:/www/ \
-d kinghtdom/hadoop3.3-cluster

docker run -it --name hadoop03 \
--network hadoop \
--ip 172.18.0.4 \
-h node3.kinghtdom.com \
-v /Users/edgar/Documents/www/BigData:/www/ \
-d kinghtdom/hadoop3.3-cluster
