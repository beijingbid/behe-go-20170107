#!/bin/bash -x
cd /data/monitorlogs
pkill DmpRealtimeToMysql || true
usleep 100000
./DmpRealtimeToMysql &
