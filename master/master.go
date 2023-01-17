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

type IntermediateFile struct {
	Path      string
	Partition int
}

type Task struct {
	Id                int
	Type              TaskType
	Status            TaskStatus
	IntermediateFiles []IntermediateFile
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

type State struct {
	M                      int
	R                      int
	InputFileName          string
	MapTasks               []Task
	ReduceTasks            []Task
	Workers                []Worker
	MapTasksAssignments    map[string]int                // WorkerAddress -> TaskId
	ReduceTasksAssignments map[string]int                // WorkerAddress -> TaskId
	IntermediateFiles      map[string][]IntermediateFile // WorkerAddress -> []IntermediateFile
	mutex                  sync.Mutex
}

func (state *State) updateWorkerStatus(workerAddress string, newStatus Status) {
	state.mutex.Lock()
	defer state.mutex.Unlock()

	for i, worker := range state.Workers {
		if worker.Address == workerAddress {
			state.Workers[i].Status = newStatus
		}
	}
}

func (state *State) assignMapTaskToWorker(taskId int, workerAddress string) {
	state.mutex.Lock()
	defer state.mutex.Unlock()

	for i, mapTask := range state.MapTasks {
		if mapTask.Id == taskId {
			state.MapTasksAssignments[workerAddress] = taskId
			mapTask.Status = InProgress
			state.MapTasks[i] = mapTask
			break
		}
	}
}

func (state *State) assignReduceTaskToWorker(taskId int, workerAddress string) {
	state.mutex.Lock()
	defer state.mutex.Unlock()

	for i, reduceTask := range state.ReduceTasks {
		if reduceTask.Id == taskId {
			state.ReduceTasksAssignments[workerAddress] = taskId
			reduceTask.Status = InProgress
			state.ReduceTasks[i] = reduceTask
			break
		}
	}
}

func (state *State) resetTaskOnWorker(workerAdress string) {
	state.mutex.Lock()
	defer state.mutex.Unlock()

	mapTaskId, hasMapTask := state.MapTasksAssignments[workerAdress]
	if hasMapTask {
		delete(state.MapTasksAssignments, workerAdress)
		mapTask := state.MapTasks[mapTaskId]
		mapTask.Status = Idle
		state.IntermediateFiles[workerAdress] = nil
		state.MapTasks[mapTaskId] = mapTask
	}

	reduceTaskId, hasReduceTask := state.ReduceTasksAssignments[workerAdress]
	if hasReduceTask {
		delete(state.ReduceTasksAssignments, workerAdress)
		reduceTask := state.ReduceTasks[reduceTaskId]
		if reduceTask.Status != Completed {
			reduceTask.Status = Idle
			state.MapTasks[reduceTaskId] = reduceTask
		}
	}
}

func (state *State) completeMapTask(
	taskId int,
	workerAddress string,
	intermediateFiles []IntermediateFile,
) {
	state.mutex.Lock()
	defer state.mutex.Unlock()

	mapTask := state.MapTasks[taskId]
	mapTask.Status = Completed
	state.MapTasks[taskId] = mapTask

	newIntermediateFiles := append(state.IntermediateFiles[workerAddress], intermediateFiles...)
	state.IntermediateFiles[workerAddress] = newIntermediateFiles
}

type MasterServer struct {
	state *State
}

type MapTaskCompletionRequest struct {
	TaskId            int
	WorkerAddress     string
	IntermediateFiles []IntermediateFile
}
type MapTaskCompletionResponse struct {
	Error string
}

func (masterServer *MasterServer) NotifyMapTaskCompletion(
	args MapTaskCompletionRequest,
	reply *MapTaskCompletionResponse,
) error {
	state := masterServer.state
	state.completeMapTask(args.TaskId, args.WorkerAddress, args.IntermediateFiles)
	return nil
}

type HeartbeatRequest struct{}
type HeartbeatReply struct{}

type MapTaskStartRequest struct {
	InputSplitFileName string
}
type MapTaskStartReply struct{}

type ReduceIntermediateFile struct {
	Path          string
	WorkerAddress string
}
type ReduceTaskStartRequest struct {
	IntermediateFiles []ReduceIntermediateFile
}
type ReduceTaskStartReply struct{}

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

	state := State{
		M:                      m,
		R:                      r,
		InputFileName:          inputFileName,
		MapTasks:               mapTasks,
		ReduceTasks:            reduceTasks,
		Workers:                workers,
		MapTasksAssignments:    map[string]int{},
		ReduceTasksAssignments: map[string]int{},
	}

	go watchWorkersStatus(&state)
	go runMapTasks(&state)
	go runReduceTasks(&state)
	go startMasterServer("localhost:6432", &MasterServer{&state})

	for {
		time.Sleep(5 * time.Second)
	}
}

func watchWorkersStatus(state *State) {
	for {
		for _, worker := range state.Workers {
			status := fetchWorkerStatus(worker)

			if status != WorkerUp {
				_, workerHasMapTaskAssigned :=
					state.MapTasksAssignments[worker.Address]
				_, workerHasReduceTaskAssigned :=
					state.ReduceTasksAssignments[worker.Address]
				if workerHasMapTaskAssigned || workerHasReduceTaskAssigned {
					state.resetTaskOnWorker(worker.Address)
				}
			}

			state.updateWorkerStatus(worker.Address, status)
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

func runMapTasks(state *State) {
	for {
		for _, mapTask := range state.MapTasks {
			if mapTask.Status != Idle {
				continue
			}

			for _, worker := range state.Workers {
				if worker.Status != WorkerUp {
					continue
				}

				_, workerHasTaskAssigned := state.MapTasksAssignments[worker.Address]
				if workerHasTaskAssigned {
					continue
				}

				inputSplitFileName := state.InputFileName + "_" + strconv.Itoa(mapTask.Id)
				err := runMapTaskOnWorker(worker, mapTask, inputSplitFileName)
				if err == nil {
					state.assignMapTaskToWorker(mapTask.Id, worker.Address)
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

func runReduceTasks(state *State) {
	for {
		allMapTasksAreCompleted := true
		for _, mapTask := range state.MapTasks {
			if mapTask.Status != Completed {
				allMapTasksAreCompleted = false
				break
			}
		}

		if !allMapTasksAreCompleted {
			continue
		}

		for reduceTaskPartition, reduceTask := range state.ReduceTasks {
			if reduceTask.Status != Idle {
				continue
			}

			for _, worker := range state.Workers {
				_, isWorkerWithTaskAssigned := state.ReduceTasksAssignments[worker.Address]
				if worker.Status != WorkerUp || isWorkerWithTaskAssigned {
					continue
				}

				intermediateFiles := make([]IntermediateFile, 0)
				for key := range state.IntermediateFiles {
					for _, intermediateFile := range state.IntermediateFiles[key] {
						if intermediateFile.Partition == reduceTaskPartition {
							intermediateFiles =
								append(intermediateFiles, intermediateFile)
						}
					}
				}

				err := runReduceTaskOnWorker(worker, reduceTask, intermediateFiles)
				if err != nil {
					state.assignReduceTaskToWorker(reduceTask.Id, worker.Address)
				}
			}
		}
	}
}

func runReduceTaskOnWorker(
	worker Worker,
	reduceTask Task,
	intermediateFiles []IntermediateFile,
) error {
	if reduceTask.Type != MapTask {
		return errors.New("task type must be 'ReduceTask'")
	}

	conn, client, err := connectTo(worker.Address)
	if err != nil {
		return err
	}
	defer (*conn).Close()
	defer client.Close()

	log.Printf("Sending task %v to '%v'...\n", reduceTask.Id, worker.Address)

	reduceIntermediateFiles := make([]ReduceIntermediateFile, len(intermediateFiles))
	for i, intermediateFile := range intermediateFiles {
		reduceIntermediateFiles[i] = ReduceIntermediateFile{
			Path:          intermediateFile.Path,
			WorkerAddress: worker.Address,
		}
	}

	reduceTaskStartRequest := ReduceTaskStartRequest{reduceIntermediateFiles}
	reduceTaskStartReply := ReduceTaskStartReply{}
	err = client.Call("WorkerTask.ReduceTaskStart", reduceTaskStartRequest, &reduceTaskStartReply)
	if err != nil {
		log.Printf("Could not receive WorkerTask.MapTaskStart reply from worker: '%v'\n", err)
		return err
	}

	return nil
}

func startMasterServer(serverAddress string, masterServer *MasterServer) {
	rpcServer := rpc.NewServer()
	err := rpcServer.Register(masterServer)
	if err != nil {
		log.Fatalf("Could not register master's RPC methods.\nReason: '%v'", err)
	}

	listener, err := net.Listen("tcp", serverAddress)
	if err != nil {
		log.Fatalf("Could not start master's server on address '%v'.\nReason: '%v'", serverAddress, err)
		os.Exit(-1)
	}

	for {
		connection, err := listener.Accept()
		if err != nil {
			log.Printf("Could not accept connection from worker.\nReason: '%v'", err)
			continue
		}

		go rpc.ServeConn(connection)
	}
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
