#!/bin/bash
cat /tmp/hosts >> /etc/hosts
#bash -c /opt/bitnami/scripts/spark/entrypoint.sh
bash /opt/bitnami/scripts/spark/run.sh