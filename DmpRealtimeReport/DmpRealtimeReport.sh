#!/bin/bash -x
cd /data/monitorlogs
pkill DmpRealtimeReport || true
usleep 100000
./DmpRealtimeReport &
