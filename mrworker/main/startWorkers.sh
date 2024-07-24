#!/usr/bin/env bash
app=$1 

# ./mrworker $app 192.168.1.109 8000 192.168.1.109 8002

./mrworker $app 192.168.1.109 8000 192.168.1.109 8002 > logs/worker1.log &
./mrworker $app 192.168.1.109 8000 192.168.1.109 8003 > logs/worker2.log &
./mrworker $app 192.168.1.109 8000 192.168.1.109 8004 > logs/worker3.log &
./mrworker $app 192.168.1.109 8000 192.168.1.109 8005 > logs/worker4.log &
./mrworker $app 192.168.1.109 8000 192.168.1.109 8006 > logs/worker5.log &