#!/bin/bash
#echo $1 | sed "s/\(\(\/\)\|\(\?\)\|\(\&\)\)/\\\\\1/g"

if [ ! -d /var/log/nginx ]
then
    mkdir /var/log/nginx
fi

docker stop bigdata_master bigdata_worker1 bigdata_worker2
docker rm bigdata_master bigdata_worker1 bigdata_worker2
#docker image rm kinghtdom/hadoop3.3-spark3.2:3
docker build -t kinghtdom/hadoop3.3-spark3.2:3 .
docker-compose up -d