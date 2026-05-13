#!/bin/bash
# 看门狗：每 10s 检测 18080 是否监听，挂了就 setsid 拉起 server.py。
# 用法：setsid bash watchdog.sh > logs/watchdog.out 2>&1 &
cd /niuniu869_dev/lilibear_world
while true; do
  if ! ss -tln '( sport = :18080 )' | grep -q LISTEN; then
    echo "[$(date '+%H:%M:%S')] 18080 not listening, restarting server.py..."
    setsid python3 -u server.py >> logs/server.out 2>&1 < /dev/null &
    sleep 3
  fi
  sleep 10
done
