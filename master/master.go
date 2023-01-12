package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type TaskStatus int

const (
	Idle = 1
	InProgress
	Completed
)

type TaskType int

const (
	MapTask    = 1
	ReduceTask = 2
)

type Task struct {
	Id     int
	Type   TaskType
	Status TaskStatus
}

type Status int

const (
	WorkerDown = 1 // Worker is available but worker program is down
	WorkerUp   = 2 // Worker is available and worker program is up
)

type Worker struct {
	Address string
	Status  Status
}

type MasterState struct {
	m                      int
	r                      int
	inputFileName          string
	mapTasks               []Task
	reduceTasks            []Task
	workers                []Worker
	mapTasksAssignments    map[string]int // WorkerAddress -> TaskId
	reduceTasksAssignments map[string]int // WorkerAddress -> TaskId
	mutex                  sync.Mutex
}

type HeartbeatRequest struct{}
type HeartbeatReply struct{}

type MapTaskStartRequest struct {
	InputSplitFileName string
}

type MapTaskStartReply struct{}

func (masterState *MasterState) updateWorkerStatus(workerAddress string, newStatus Status) {
	masterState.mutex.Lock()
	defer masterState.mutex.Unlock()

	for i, worker := range masterState.workers {
		if worker.Address == workerAddress {
			masterState.workers[i].Status = newStatus
		}
	}
}

func (masterState *MasterState) assignMapTaskToWorker(taskId int, workerAddress string) {
	masterState.mutex.Lock()
	defer masterState.mutex.Unlock()

	for _, mapTask := range masterState.mapTasks {
		if mapTask.Id == taskId {
			masterState.mapTasksAssignments[workerAddress] = taskId
			break
		}
	}
}

func (masterState *MasterState) resetTaskOnWorker(workerAdress string) {
	masterState.mutex.Lock()
	defer masterState.mutex.Unlock()

	mapTaskId, hasMapTask := masterState.mapTasksAssignments[workerAdress]
	if hasMapTask {
		delete(masterState.mapTasksAssignments, workerAdress)
		mapTask := masterState.mapTasks[mapTaskId]
		mapTask.Status = Idle
		masterState.mapTasks[mapTaskId] = mapTask
	}

	reduceTaskId, hasReduceTask := masterState.reduceTasksAssignments[workerAdress]
	if hasReduceTask {
		delete(masterState.reduceTasksAssignments, workerAdress)
		reduceTask := masterState.reduceTasks[reduceTaskId]
		if reduceTask.Status != Completed {
			reduceTask.Status = Idle
			masterState.mapTasks[reduceTaskId] = reduceTask
		}
	}
}

func main() {
	if len(os.Args) < 5 {
		log.Println("Invalid argument count")
		log.Println("Format expected: command-name <m> <r> " +
			"<input-file-name> <worker-address-1> ... <worker-address-n>")
		os.Exit(-1)
	}

	m, mParseError := strconv.Atoi(os.Args[1])
	if mParseError != nil {
		log.Printf("Invalid argument '%v' for parameter <m>", os.Args[1])
		os.Exit(-1)
	}

	r, rParseError := strconv.Atoi(os.Args[2])
	if rParseError != nil {
		log.Printf("Invalid argument '%v' for parameter <r>", os.Args[2])
		os.Exit(-1)
	}

	inputFileName := os.Args[3]

	workerAddresses := os.Args[4:]
	if len(workerAddresses) == 0 {
		log.Printf("No worker addresses were supplied")
		os.Exit(-1)
	}

	mapTasks := make([]Task, m)
	for i := 0; i < m; i++ {
		mapTasks[i] = Task{Id: i, Type: MapTask, Status: Idle}
	}

	reduceTasks := make([]Task, r)
	for i := 0; i < r; i++ {
		reduceTasks[i] = Task{Id: i, Type: MapTask, Status: Idle}
	}

	workers := make([]Worker, len(workerAddresses))
	for i := 0; i < len(workers); i++ {
		workers[i] = Worker{Address: workerAddresses[i], Status: WorkerDown}
	}

	masterState := MasterState{
		m:                      m,
		r:                      r,
		inputFileName:          inputFileName,
		mapTasks:               mapTasks,
		reduceTasks:            reduceTasks,
		workers:                workers,
		mapTasksAssignments:    map[string]int{},
		reduceTasksAssignments: map[string]int{},
	}

	go watchWorkersStatus(&masterState)
	go runMapTasks(&masterState)

	for {
		time.Sleep(5 * time.Second)
	}
}

func watchWorkersStatus(masterState *MasterState) {
	for {
		for _, worker := range masterState.workers {
			status := fetchWorkerStatus(worker)
			if status != WorkerUp {
				_, workerHasMapTaskAssigned :=
					masterState.mapTasksAssignments[worker.Address]
				_, workerHasReduceTaskAssigned :=
					masterState.reduceTasksAssignments[worker.Address]
				if workerHasMapTaskAssigned || workerHasReduceTaskAssigned {
					masterState.resetTaskOnWorker(worker.Address)
				}
			}

			masterState.updateWorkerStatus(worker.Address, status)
		}

		time.Sleep(5 * time.Second)
	}
}

func fetchWorkerStatus(worker Worker) Status {
	conn, client, err := connectTo(worker.Address)
	if err != nil {
		log.Printf("Could not connect to worker '%v' to send heartbeat message.", worker.Address)
		return WorkerDown
	}
	defer client.Close()
	defer (*conn).Close()

	if worker.Status == WorkerDown {
		log.Printf("Sending heartbeat to worker '%v'...\n", worker.Address)
	}

	err = client.Call("WorkerHeartbeat.GetHeartbeat", HeartbeatRequest{}, &HeartbeatReply{})
	if err != nil {
		log.Printf("Could not receive heartbeat response from worker '%v': '%v'"+
			". Changing worker status to DOWN.\n", worker.Address, err)
		return WorkerDown
	}

	return WorkerUp
}

func runMapTasks(masterState *MasterState) {
	for {
		for _, mapTask := range masterState.mapTasks {
			if mapTask.Status != Idle {
				continue
			}

			for _, worker := range masterState.workers {
				if worker.Status != WorkerUp {
					continue
				}

				_, workerHasTaskAssigned := masterState.mapTasksAssignments[worker.Address]
				if workerHasTaskAssigned {
					continue
				}

				inputSplitFileName := masterState.inputFileName + "_" + strconv.Itoa(mapTask.Id)
				err := runMapTaskOnWorker(worker, mapTask, inputSplitFileName)
				if err == nil {
					masterState.assignMapTaskToWorker(mapTask.Id, worker.Address)
				}
			}
		}

		time.Sleep(10 * time.Second)
	}
}

func runMapTaskOnWorker(
	worker Worker,
	mapTask Task,
	inputSplitFileName string,
) error {
	if mapTask.Type != MapTask {
		return errors.New("task type must be 'MapTask'")
	}

	conn, client, err := connectTo(worker.Address)
	if err != nil {
		return err
	}
	defer (*conn).Close()
	defer client.Close()

	log.Printf("Sending task %v to '%v'...\n", mapTask.Id, worker.Address)
	mapTaskStartRequest := MapTaskStartRequest{
		InputSplitFileName: inputSplitFileName,
	}
	mapTaskStartReply := MapTaskStartReply{}
	err = client.Call("WorkerTask.MapTaskStart", mapTaskStartRequest, &mapTaskStartReply)
	if err != nil {
		log.Printf("Could not receive WorkerTask.MapTaskStart reply from worker: '%v'\n", err)
		return err
	}

	return nil
}

func connectTo(address string) (*net.Conn, *rpc.Client, error) {
	log.Printf("Dialing host: '%v'...\n", address)
	dialer := net.Dialer{Timeout: 5 * time.Second}

	_, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := dialer.Dial("tcp", address)
	if err != nil {
		log.Printf("Host '%v' is unreachable: %v.\n", address, err)
		return nil, nil, err
	}

	return &conn, rpc.NewClient(conn), err
}
