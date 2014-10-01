#!/usr/bin/env sh
rm /tmp/logdata
touch /tmp/logdata
tail -f /tmp/logdata | nc -lk 7777 &
TAIL_NC_PID=$!
cat ./files/fake_logs/log1.log >> /tmp/logdata
sleep 5
cat ./files/fake_logs/log2.log >> /tmp/logdata
sleep 1
cat ./files/fake_logs/log1.log >> /tmp/logdata
sleep 2
cat ./files/fake_logs/log1.log >> /tmp/logdata
sleep 3
sleep 20
kill $TAIL_NC_PID
