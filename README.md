# Map Reduce
This is an implementation of map reduce algorithm according to the paper: [1]Jeffrey, Dean, Sanjay, & Ghemawat. (2008). Mapreduce: simplified data processing on large clusters. Communications of the Acm.

## Algorithm
1. The task is divided to M splits (M map tasks).
2. Intermediate space is partitioned to R pieces (R reduce tasks).
3. The coordinator is responsible for managing the tasks (idle, in-process, completed) and assigning the tasks to workers.
4. Map worker: Read a map file from coordinator (or a specific location), finish the task and write the intermediate results locally.
5. Reduce worker: Read intermediate results remotely, finish the task and write results to coordinator (or a specific location).
6. Worker failure: If a map worker dies, the map task needs to be assigned to a new worker. For reduce worker, it is not neccessary.
7. Coordinator failure: States of data structures in coordinator need to be saved to a checkpoint periodly. If the coordinator dies, a new coordinator should be re-started based on the checkpoint.

## Build
Go version:
```
go version go1.22.5 linux/amd64
```
Coordinator:
```
cd mrcoordinator
go mod init mrcoordinator
go build
```
Worker:
```
cd mrworker
go mod init mrworker
cd main
go build -o mrworker
```
MrApps:
```
cd mrworker/main
mkdir apps
bash build_mrapps.sh
```

## Run
Coordinator:
```
cd mrcoordinator
mkdir results
./mrcoordinator task/* (can be changed to other files)
```
Worker:
```
cd mrworker/main
mkdir logs
bash startWorkers.sh apps/wc.so
```