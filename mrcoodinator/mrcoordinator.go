package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

func main() {
	var port string
	var fileStart int
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	} else if len(os.Args) == 2 {
		if _, err := strconv.Atoi(os.Args[1]); err == nil {
			fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
			os.Exit(1)
		} else {
			port = ""
			fileStart = 1
		}
	} else {
		if _, err := strconv.Atoi(os.Args[1]); err == nil {
			port = os.Args[1]
			fileStart = 2
		} else {
			port = ""
			fileStart = 1
		}
	}

	m := MakeCoordinator(os.Args[fileStart:], 10, port)
	for !m.Done() {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
