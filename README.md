# Map Reduce
This is an implementation of map reduce algorithm according to the paper: [1]Jeffrey, Dean, Sanjay, & Ghemawat. (2008). Mapreduce: simplified data processing on large clusters. Communications of the Acm.

## Algorithm
1. The task is divided to M splits (M map tasks).
2. Intermediate space is partitioned to R pieces (R reduce tasks).
3. The coordinator is responsible for managing the tasks (idle, in-process, completed) and assigning the tasks to workers.
4. Map worker: Read a map file from coordinator (or a specific location), finish the task and write the intermediate results locally.
5. Reduce worker: Read intermediate results remotely, finish the task and write results to coordinator (or a specific location).
6. Worker failure: If a map worker dies, the map task needs to be assigned to a new worker and reduce tasks need to be modified. For a reduce worker, it is enough to only assign the task to a new worker.
7. Coordinator failure: States of data structures in coordinator need to be saved to a checkpoint periodly. If the coordinator dies, a new coordinator should be re-started based on the checkpoint.

## Implementation
### Data structures in coordinator:
```
mapTasks             []*MapTask
reduceTasks          []*ReduceTask
inProcessMapTasks    map[int]*MapTask
inProcessReduceTasks map[int]*ReduceTask
completeMapTasks     map[string][]*MapTask
```

### Normal case (no failures)
Worker request (taskCall) -> Coordinator (HandleRequest), change states of data structures. -> Worker (doMap or doReduce), (noticeCall) -> Coordinator (HandleNotice), change states of data structures.
1. Map stage (map tasks left or in-progress map tasks left).
2. Reduce stage (reduce tasks left or in-progress reduce tasks left).

### Failure handling
Coordinator (checkWorkers)
1. In map stage and a map worker died (completed or in-progress):  
Switch the map task back to unassigned state and change files in reduce tasks to 'null' if it is completed map task.
2. In reduce stage and a reduce worker died (not a completed map worker): Change states and reschedule it.
3. In reduce stage and a completed map worker died:  
a. 把这个机器上完成的map tasks都放回到未完成列表中去。  
b. 这些map tasks会被重新分配并做完，更新reduce tasks。  
c. Reduce worker会缓存做过的reduce，并重新请求这个reduce task。当map tasks被重新做完之后，这个reduce task会重新分配给它，它会继续做完。

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