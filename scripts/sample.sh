#!/usr/bin/env bash
# Sample used disk (bytes) and used memory (bytes) every 5 s until killed.
while true; do
  echo "$(date +%s) $(df -B1 --output=used / | tail -1) $(free -b | awk '/Mem/{print $3}')" >> "$1"
  sleep 5
done
