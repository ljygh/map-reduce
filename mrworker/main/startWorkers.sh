#!/usr/bin/env bash
app=$1 

./mrworker $app 192.168.31.193 8000 8002 > worker1.log &
./mrworker $app 192.168.31.193 8000 8003 > worker2.log &
./mrworker $app 192.168.31.193 8000 8004 > worker3.log &
./mrworker $app 192.168.31.193 8000 8005 > worker4.log &
./mrworker $app 192.168.31.193 8000 8006 > worker5.log &