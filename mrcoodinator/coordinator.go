package main

import (
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

type MapTask struct {
	mapID   int
	file    string
	runTime int
}

type ReduceTask struct {
	reduceID int
	files    []string
	runTime  int
}

type Coordinator struct {
	// Your definitions here.
	ch                   chan int
	nReduce              int
	mapTasks             []*MapTask
	reduceTasks          []*ReduceTask
	inProcessMapTasks    map[int]*MapTask
	inProcessReduceTasks map[int]*ReduceTask
	completeMapTasks     map[string][]*MapTask
}

// var cSock string = coordinatorSock()
var port string = "8000"
var isHandlingCompleteMapWorkerDie bool = false
var httpDir string = "./"

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int, cPort string) *Coordinator {
	log.SetOutput(os.Stdout)
	// log.SetOutput(ioutil.Discard)
	// outFile, _ := os.Open("../coordinator.log")
	// log.SetOutput(outFile)
	log.SetFlags(log.Lshortfile)
	log.Println("Init coordinator")

	// set port
	if len(cPort) != 0 {
		port = cPort
	}

	// remove all mr-out-* files
	remove_mr_out_files_and_dirs()

	// init coordinator data structure
	c := Coordinator{}
	c.ch = make(chan int, 1)
	c.nReduce = nReduce
	for i, file := range files {
		mapTask := MapTask{}
		mapTask.mapID = i
		mapTask.file = file
		mapTask.runTime = 0
		c.mapTasks = append(c.mapTasks, &mapTask)
	}
	for i := 0; i < nReduce; i++ {
		reduceTask := ReduceTask{}
		reduceTask.reduceID = i
		reduceTask.runTime = 0
		for i := 0; i < len(files); i++ {
			reduceTask.files = append(reduceTask.files, "null")
		}
		c.reduceTasks = append(c.reduceTasks, &reduceTask)
	}
	c.inProcessMapTasks = make(map[int]*MapTask)
	c.inProcessReduceTasks = make(map[int]*ReduceTask)
	c.completeMapTasks = make(map[string][]*MapTask)
	c.printCoordinator()

	// start servers and worker failure checking thread
	c.rpcServer()
	go httpServer()
	go c.checkWorkers()
	return &c
}

// Remove all mr-out-* files
func remove_mr_out_files_and_dirs() {
	files, err := ioutil.ReadDir(".")
	if err != nil {
		log.Fatal("Error while reading dir: ", err)
	}

	for _, file := range files {
		filename := file.Name()
		if file.IsDir() {
			os.RemoveAll("./" + filename)
		} else if strings.HasPrefix(filename, "mr-out") {
			os.Remove("./" + filename)
		}
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) rpcServer() {
	log.Print("Set rpc server")
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":"+port)

	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Print("Listen to:", port)
	go http.Serve(l, nil)
}

// start a thread that listens for http request from worker.go
func httpServer() {
	log.Print("Set http server")
	rpcPortN, err := strconv.Atoi(port)
	if err != nil {
		log.Fatal("Fail to convert rpc port to integer")
	}
	httpPortN := rpcPortN + 1
	httpPort := strconv.Itoa(httpPortN)

	http.HandleFunc("/", httpHandler)
	log.Print("Listen to:", httpPort)
	log.Fatal(http.ListenAndServe(":"+httpPort, nil))
}

// Http handler
func httpHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method == "GET" {
		log.Print("Receive GET")
		filename := req.URL.Path
		log.Println("Request for file:", filename)
		filename = httpDir + filename

		if _, err := os.Stat(filename); os.IsNotExist(err) {
			log.Println("Requested file not exist")
			w.WriteHeader(404)
			return
		}

		fileBytes, err := ioutil.ReadFile(filename)
		if err != nil {
			log.Fatal("Error while opening file: ", err)
		}
		w.WriteHeader(200)
		w.Write(fileBytes)

	} else if req.Method == "POST" { // store file after getting post
		log.Println("Receive POST")
		filename := req.URL.Path
		log.Println("Receive file:", filename)
		filename = "." + filename

		if _, err := os.Stat(filename); os.IsNotExist(err) {
			file, err := os.Create(filename)
			if err != nil {
				log.Fatal("Error while creating file: ", err)
			}
			body, err := ioutil.ReadAll(req.Body)
			if err != nil {
				log.Fatal(err)
			}
			_, err = file.Write(body)
			if err != nil {
				log.Fatal(err)
			}
			file.Close()

			w.WriteHeader(200)
		} else {
			log.Print("File already exists")
			w.WriteHeader(400)
		}
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) HandleRequest(args *int, reply *Task) error {
	log.Println("Handle request")
	c.ch <- 1
	if len(c.mapTasks) != 0 { // map task left
		task := c.mapTasks[0]
		reply.TaskType = 0
		reply.TaskID = task.mapID
		reply.Files = append(reply.Files, task.file)
		reply.NReduce = c.nReduce

		c.mapTasks = c.mapTasks[1:]
		c.inProcessMapTasks[task.mapID] = task

		log.Print("Response, task type: ", reply.TaskType, " task ID: ", reply.TaskID, " task file: ", reply.Files, " NReduce: ", reply.NReduce)
		c.printCoordinator()
	} else if len(c.inProcessMapTasks) != 0 { // no map task left but some map task is executing
		reply.TaskType = 2
	} else if _, ok := c.inProcessReduceTasks[*args]; ok && isHandlingCompleteMapWorkerDie {
		reply.TaskType = 2
	} else if task, ok := c.inProcessReduceTasks[*args]; ok { // request for executing reduce task
		log.Print("Update file locations for in process reduce task worker")
		reply.TaskType = 1
		reply.TaskID = task.reduceID
		reply.Files = append(reply.Files, task.files...)
		reply.NReduce = c.nReduce

		log.Print("Response, task type: ", reply.TaskType, " task ID: ", reply.TaskID, " task file: ", reply.Files, " NReduce: ", reply.NReduce)
		c.printCoordinator()
	} else if len(c.reduceTasks) != 0 { // reduce task left
		task := c.reduceTasks[0]
		reply.TaskType = 1
		reply.TaskID = task.reduceID
		reply.Files = append(reply.Files, task.files...)
		reply.NReduce = c.nReduce

		c.reduceTasks = c.reduceTasks[1:]
		c.inProcessReduceTasks[task.reduceID] = task

		log.Print("Response, task type: ", reply.TaskType, " task ID: ", reply.TaskID, " task file: ", reply.Files, " NReduce: ", reply.NReduce)
		c.printCoordinator()
	} else {
		reply.TaskType = 2
	}
	<-c.ch
	return nil
}

func (c *Coordinator) HandleNotice(args *NoticeArgs, reply *bool) error {
	log.Print("Handle notice")
	c.ch <- 1
	log.Print("Notice worker address: ", args.WorkerAddr)
	log.Print("Task ID: ", args.TaskID)
	if _, ok := c.inProcessMapTasks[args.TaskID]; args.TaskType == 0 && ok { // map task done
		log.Printf("Map task %v done", args.TaskID)

		if _, ok := c.completeMapTasks[args.WorkerAddr]; ok {
			taskSlice := c.completeMapTasks[args.WorkerAddr]
			taskSlice = append(taskSlice, c.inProcessMapTasks[args.TaskID])
			c.completeMapTasks[args.WorkerAddr] = taskSlice
		} else {
			taskSlice := []*MapTask{}
			taskSlice = append(taskSlice, c.inProcessMapTasks[args.TaskID])
			c.completeMapTasks[args.WorkerAddr] = taskSlice
		}
		delete(c.inProcessMapTasks, args.TaskID)

		for _, task := range c.reduceTasks {
			task.files[args.TaskID] = args.Files[task.reduceID]
		}
		if isHandlingCompleteMapWorkerDie { // update inProcessReduceTasks files
			for _, task := range c.inProcessReduceTasks {
				task.files[args.TaskID] = args.Files[task.reduceID]
			}
		}

		if len(c.mapTasks) == 0 && len(c.inProcessMapTasks) == 0 { // change back  to normal state from re-executing map task
			isHandlingCompleteMapWorkerDie = false
		}

		c.printCoordinator()
		*reply = true
	} else if _, ok = c.inProcessReduceTasks[args.TaskID]; args.TaskType == 1 && ok { // reduce task done
		log.Printf("Reduce task %v done", args.TaskID)
		delete(c.inProcessReduceTasks, args.TaskID)
		c.printCoordinator()
		*reply = true
	} else if args.TaskType == 0 {
		log.Printf("Map task %v time out", args.TaskID)
		*reply = false
	} else if args.TaskType == 1 {
		log.Printf("Reduce task %v time out", args.TaskID)
		*reply = false
	}
	<-c.ch
	return nil
}

func (c *Coordinator) checkWorkers() {
	for {
		c.ch <- 1

		if !isHandlingCompleteMapWorkerDie {
			for workerAddr, mapTaskSlice := range c.completeMapTasks { // check complete map workers
				conn, err := net.Dial("tcp", workerAddr)
				if err != nil && (len(c.mapTasks) != 0 || len(c.inProcessMapTasks) != 0) {
					log.Print("Complete map task worker dies: ", workerAddr)
					log.Print("Map stage")
					for _, mapTask := range mapTaskSlice {
						mapTask.runTime = 0
						c.mapTasks = append(c.mapTasks, mapTask)
					}
					delete(c.completeMapTasks, workerAddr)

					for _, reduceTask := range c.reduceTasks {
						for i, file := range reduceTask.files {
							if strings.HasPrefix(file, "http://"+workerAddr) {
								reduceTask.files[i] = "null"
							}
						}
					}

					c.printCoordinator()
				} else if err != nil {
					log.Print("Complete map task worker dies: ", workerAddr)
					log.Print("Reduce stage")
					for _, mapTask := range mapTaskSlice {
						mapTask.runTime = 0
						c.mapTasks = append(c.mapTasks, mapTask)
					}
					delete(c.completeMapTasks, workerAddr)

					isHandlingCompleteMapWorkerDie = true
					c.printCoordinator()
				} else {
					conn.Close()
				}
			}
		}

		for taskID, task := range c.inProcessMapTasks { // check inProcessMapTasks
			task.runTime++
			c.inProcessMapTasks[taskID] = task
			if task.runTime >= 10 {
				delete(c.inProcessMapTasks, taskID)
				task.runTime = 0
				c.mapTasks = append(c.mapTasks, task)
				log.Printf("Map task %v idle, stop it for rescheduling", taskID)
				c.printCoordinator()
			}
		}

		if !isHandlingCompleteMapWorkerDie {
			for taskID, task := range c.inProcessReduceTasks { // check inProcessReduceTasks
				task.runTime++
				c.inProcessReduceTasks[taskID] = task
				if task.runTime >= 10 {
					delete(c.inProcessReduceTasks, taskID)
					task.runTime = 0
					c.reduceTasks = append(c.reduceTasks, task)
					log.Printf("Reduce task %v idle, stop it for rescheduling", taskID)
					c.printCoordinator()
				}
			}
		}
		<-c.ch
		time.Sleep(time.Second)
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// log.Print("Check done")
	c.ch <- 1
	ret := false

	// Your code here.
	if len(c.mapTasks) == 0 && len(c.reduceTasks) == 0 && len(c.inProcessMapTasks) == 0 && len(c.inProcessReduceTasks) == 0 {
		ret = true
	}

	<-c.ch
	return ret
}

func (c *Coordinator) printCoordinator() {
	log.Println()
	log.Println("Print coordinator states")
	log.Println("NReduce:", c.nReduce)
	if len(c.mapTasks) > 0 {
		log.Println("Map tasks:")
		for _, mapTask := range c.mapTasks {
			log.Printf("%v ", *mapTask)
		}
		log.Println()
	}
	if len(c.reduceTasks) > 0 {
		log.Println("Reduce tasks:")
		for _, reduceTask := range c.reduceTasks {
			log.Printf("%v ", *reduceTask)
		}
		log.Println()
	}
	if len(c.inProcessMapTasks) > 0 {
		log.Println("In process Map tasks:")
		for workerAddr, mapTask := range c.inProcessMapTasks {
			log.Printf("%v %v ", workerAddr, *mapTask)
		}
		log.Println()
	}
	if len(c.inProcessReduceTasks) > 0 {
		log.Println("In process Reduce tasks:")
		for workerAddr, reduceTask := range c.inProcessReduceTasks {
			log.Printf("%v %v ", workerAddr, *reduceTask)
		}
		log.Println()
	}
	if len(c.completeMapTasks) > 0 {
		log.Println("Complete map tasks:")
		for workerAddr, mapTaskSlice := range c.completeMapTasks {
			log.Printf("%v %v ", workerAddr, mapTaskSlice)
		}
		log.Println()
	}
	log.Println()
}
