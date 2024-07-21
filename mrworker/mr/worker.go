package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// var directory string = "../main/"

var directory string = ""

var cIP string
var cPort string
var wIP string = "192.168.31.193"
var wPort string = "8002"

// cache for interupted reduce task
var hasCacheIntermedia bool = false
var cacheIntermedia []KeyValue
var cacheIndex int
var reduceID int

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string, coordinatorIP string, coordinatorPort string, workerIP string, workerPort string) {
	log.SetOutput(os.Stdout)
	// log.SetOutput(ioutil.Discard)
	log.SetFlags(log.Lshortfile)
	log.Print("Worker starts")

	// set IP and port for both coordinator and worker
	cIP = coordinatorIP
	cPort = coordinatorPort
	if len(workerPort) != 0 {
		wPort = workerPort
	}
	if len(workerIP) != 0 {
		wIP = workerIP
	}
	log.Print("Local IP address: ", wIP)

	// Init directory for reduce files
	remove_dirs()
	dirName := wIP + "-" + wPort
	os.Mkdir(dirName, 0777)
	directory = "./" + dirName + "/"

	// Set worker http server
	go workerHttpServer()

	for {
		log.Print()
		log.Print("Request for task")
		task := Task{}
		res := taskCall(&task)
		if res == 0 {
			log.Fatal("Worker terminates")
		} else if res == 2 {
			log.Println("Error in rpc")
			continue
		} else if task.TaskType == 2 {
			log.Println("No task available")
			time.Sleep(5 * time.Second)
			continue
		}

		// do task
		var noticeFiles []string
		if task.TaskType == 0 { // map
			log.Print("Got map task ", task.TaskID)
			noticeFiles = doMap(mapf, task)
		} else if task.TaskType == 1 { // reduce
			log.Print("Got reduce task ", task.TaskID)
			err := doReduce(reducef, task)
			if err != nil {
				time.Sleep(time.Second)
				continue
			}
		}

		// notice
		log.Print("Task done, notice coordinator")
		noticeArgs := NoticeArgs{}
		noticeArgs.TaskType = task.TaskType
		noticeArgs.WorkerAddr = wIP + ":" + wPort
		noticeArgs.TaskID = task.TaskID
		noticeArgs.Files = noticeFiles
		var reply bool
		noticeCall(&noticeArgs, &reply)
		if task.TaskType == 1 && reply == true {
			for _, url := range task.Files {
				err := deleteFileFromWorker(url)
				if err != nil {
					log.Print("Can't delete file from worker: ", url)
				}
			}
		}

		time.Sleep(time.Second)
	}

}

// Remove all files in a directory
func remove_dirs() {
	files, err := ioutil.ReadDir(".")
	if err != nil {
		log.Fatal("Error while reading dir: ", err)
	}

	for _, file := range files {
		filename := file.Name()
		if file.IsDir() {
			os.RemoveAll("./" + filename)
		}
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func taskCall(reply *Task) int {
	c, err := rpc.DialHTTP("tcp", cIP+":"+cPort)
	if err != nil {
		log.Println("Coordinator terminated, tasks done")
		return 0
	}
	defer c.Close()
	log.Print("Dial to coordinator successfully")

	var arg int
	if hasCacheIntermedia {
		arg = reduceID
	} else {
		arg = -1
	}
	err = c.Call("Coordinator.HandleRequest", &arg, reply)
	log.Print("Call remote function")
	if err == nil {
		return 1
	}
	return 2
}

//
// send an RPC notice to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong or the coordinator response false.
//
func noticeCall(args *NoticeArgs, reply *bool) bool {
	c, err := rpc.DialHTTP("tcp", cIP+":"+cPort)
	// c, err := rpc.DialHTTP("unix", cSock)
	if err != nil {
		log.Println("Fail to notice")
		return false
	}
	defer c.Close()

	err = c.Call("Coordinator.HandleNotice", args, reply)
	if err == nil && *reply == true {
		return true
	}
	return false
}

func doMap(mapf func(string, string) []KeyValue, task Task) []string {
	// get file content from coordinator
	filename := task.Files[0]
	log.Print("Do map on file:", filename)
	content := getFileFromCoordinator(filename)

	kva := mapf(filename, string(content))

	intermediate := []KeyValue{}
	intermediate = append(intermediate, kva...)
	// log.Printf("Map result: %v", intermediate)

	// Create json files
	var interFiles []string
	for i := 0; i < task.NReduce; i++ {
		interFilename := "mr-" + strconv.Itoa(task.TaskID) + "-" + strconv.Itoa(i) + ".json"
		file, err := os.Create(directory + interFilename)
		if err != nil {
			log.Fatal("Error while creating file:", err)
		}
		file.Close()
		interFiles = append(interFiles, interFilename)
	}
	log.Printf("Create intermedia files: %v", interFiles)

	// Write to json files
	for _, kv := range intermediate {
		key := kv.Key
		reduceID := ihash(key) % task.NReduce
		file, err := os.OpenFile(directory+interFiles[reduceID], os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal("Error while opening file:", err)
		}

		err = json.NewEncoder(file).Encode(&kv)
		if err != nil {
			log.Fatal("Error while encoding KeyValue to file:", err)
		}
		file.Close()
	}
	log.Print("Write KeyValues to intermedia files")

	for i, interFileName := range interFiles {
		interFiles[i] = "http://" + wIP + ":" + wPort + "/" + interFileName
	}
	return interFiles
}

func doReduce(reducef func(string, []string) string, task Task) error {
	// request reduce files from workers; if worker dies, cache read content and continue to read left files after re-executing map task
	var intermediate []KeyValue
	if hasCacheIntermedia {
		log.Print("Continue to do reduce on files: ", task.Files)
		intermediate = cacheIntermedia
		for i := cacheIndex; i < len(task.Files); i++ {
			url := task.Files[i]
			content, err := getFileFromWorker(url)
			if err != nil {
				hasCacheIntermedia = true
				cacheIntermedia = intermediate
				cacheIndex = i
				reduceID = task.TaskID
				return err
			}
			contentReader := bytes.NewReader(content)

			dec := json.NewDecoder(contentReader)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
		}

		hasCacheIntermedia = false
	} else {
		log.Print("Do reduce on files:", task.Files)
		for i, url := range task.Files {
			content, err := getFileFromWorker(url)
			if err != nil {
				hasCacheIntermedia = true
				cacheIntermedia = intermediate
				cacheIndex = i
				reduceID = task.TaskID
				return err
			}
			contentReader := bytes.NewReader(content)

			dec := json.NewDecoder(contentReader)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
		}
	}

	sort.Sort(ByKey(intermediate))
	// log.Print("Read KeyValues:", intermediate)

	outFileName := "mr-out-" + strconv.Itoa(task.TaskID)
	outStr := ""
	log.Print("Send file ", outFileName, " to coordinator")

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		outStr += fmt.Sprintf("%v %v\n", intermediate[i].Key, output)
		i = j
	}

	outBytes := []byte(outStr)
	postFileToCoordinator(outFileName, outBytes)
	return nil
}

// Get file from coordinator
func getFileFromCoordinator(filename string) []byte {
	resp, err := http.Get("http://" + cIP + ":" + cPort + "/" + filename)
	if err != nil {
		log.Fatal("Error while getting file from coordinator:", err)
	}
	defer resp.Body.Close()
	content, err := io.ReadAll(resp.Body)
	return content
}

// Post file to coordinator
func postFileToCoordinator(filename string, fileBytes []byte) {
	fileReader := bytes.NewReader(fileBytes)
	resp, err := http.Post("http://"+cIP+":"+cPort+"/"+filename, "text/plain", fileReader)
	if err != nil {
		log.Fatal("Error while posting file to coordinator:", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	log.Print(string(body))
}

// Get file from worker
func getFileFromWorker(url string) ([]byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		log.Print("Error while getting file from worker:", err)
		return nil, err
	}
	defer resp.Body.Close()
	content, err := io.ReadAll(resp.Body)
	return content, nil
}

// Delete file from worker
func deleteFileFromWorker(url string) error {
	req, _ := http.NewRequest("DELETE", url, nil)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Print("Error while deleting file from worker:", err)
		return err
	}
	resp.Body.Close()
	return nil
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func workerHttpServer() {
	log.Print("Set http server")
	http.HandleFunc("/", workerHttpHandler)
	log.Print("Listen to:", wPort)
	log.Fatal(http.ListenAndServe(":"+wPort, nil))
}

// Http handler
func workerHttpHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method == "GET" {
		log.Print("Receive GET")
		filename := req.URL.Path
		log.Println("Request for file:", filename)
		filepath := directory + filename

		if _, err := os.Stat(filepath); os.IsNotExist(err) {
			log.Println("Requested file not exist")
			w.WriteHeader(404)
			return
		}

		fileBytes, err := ioutil.ReadFile(filepath)
		if err != nil {
			log.Fatal("Error while opening file: ", err)
		}
		w.WriteHeader(200)
		w.Write(fileBytes)
	} else if req.Method == "DELETE" {
		log.Print("Receive DELETE")
		filename := req.URL.Path
		log.Println("Request to delete file:", filename)
		filepath := directory + filename

		if _, err := os.Stat(filepath); os.IsNotExist(err) {
			log.Println("Requested file not exist")
			w.WriteHeader(404)
			return
		}

		os.Remove(filepath)
		w.WriteHeader(200)
	}
}
