#!/bin/bash
cd /data/DmpRealtimeReport
#pkill DmpRealtimeReport || true
#pkill DmpRealtimeToMysql || true
#killall DmpRealtimeReport 
#killall DmpRealtimeToMysql



usleep 10000000
./DmpRealtimeToMysql &
./DmpRealtimeReport &


