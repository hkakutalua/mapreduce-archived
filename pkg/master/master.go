package master

import (
	"context"
	"errors"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"time"

	. "github.com/henrick/mapreduce/internal/master"
	"github.com/henrick/mapreduce/pkg/data"
	"github.com/henrick/mapreduce/pkg/worker"
)

type Server struct {
	state *State
}

func Run(m int, r int, inputFilePath string, workersAddresses []string) {
	mapTasks := make([]Task, m)
	for i := 0; i < m; i++ {
		mapTasks[i] = Task{Id: i, Type: MapTask, Status: Idle}
	}

	reduceTasks := make([]Task, r)
	for i := 0; i < r; i++ {
		reduceTasks[i] = Task{Id: i, Type: ReduceTask, Status: Idle}
	}

	workers := make([]Node, len(workersAddresses))
	for i := 0; i < len(workers); i++ {
		workers[i] = Node{Address: workersAddresses[i], Status: NodeDown}
	}

	state := State{
		M:                      m,
		R:                      r,
		InputFilePath:          inputFilePath,
		MapTasks:               mapTasks,
		ReduceTasks:            reduceTasks,
		Nodes:                  workers,
		MapTasksAssignments:    map[string]int{},
		ReduceTasksAssignments: map[string]int{},
		IntermediateFiles:      map[string][]IntermediateFile{},
	}

	go watchWorkersStatus(&state)
	go runMapTasks(&state)
	go runReduceTasks(&state)
	go startMasterServer("localhost:6432", &Server{&state})

	for {
		time.Sleep(5 * time.Second)
	}
}

func watchWorkersStatus(state *State) {
	for {
		for _, node := range state.Nodes {
			status := fetchNodeStatus(node)

			if status != NodeUp {
				_, workerHasMapTaskAssigned :=
					state.MapTasksAssignments[node.Address]
				_, workerHasReduceTaskAssigned :=
					state.ReduceTasksAssignments[node.Address]
				if workerHasMapTaskAssigned || workerHasReduceTaskAssigned {
					state.ResetTaskOnNode(node.Address)
				}
			}

			state.UpdateNodeStatus(node.Address, status)
		}

		time.Sleep(5 * time.Second)
	}
}

func fetchNodeStatus(node Node) Status {
	conn, client, err := connectTo(node.Address)
	if err != nil {
		log.Printf("Could not connect to node '%v' to send heartbeat message.", node.Address)
		return NodeDown
	}
	defer client.Close()
	defer (*conn).Close()

	if node.Status == NodeDown {
		log.Printf("Sending heartbeat to node '%v'...\n", node.Address)
	}

	err = client.Call(worker.GetHeartbeat, data.HeartbeatArgs{}, &data.HeartbeatReply{})
	if err != nil {
		log.Printf("Could not receive heartbeat response from node '%v': '%v'"+
			". Changing node status to DOWN.\n", node.Address, err)
		return NodeDown
	}

	return NodeUp
}

func runMapTasks(state *State) {
	for {
		for _, mapTask := range state.MapTasks {
			if mapTask.Status != Idle {
				continue
			}

			for _, node := range state.Nodes {
				if node.Status != NodeUp {
					continue
				}

				_, workerHasTaskAssigned := state.MapTasksAssignments[node.Address]
				if workerHasTaskAssigned {
					continue
				}

				inputSplitFileName := state.InputFilePath + "_" + strconv.Itoa(mapTask.Id)
				err := runMapTaskOnNode(node, mapTask, inputSplitFileName)
				if err == nil {
					state.AssignMapTaskToNode(mapTask.Id, node.Address)
				}
			}
		}

		time.Sleep(10 * time.Second)
	}
}

func runMapTaskOnNode(
	node Node,
	mapTask Task,
	inputSplitFileName string,
) error {
	if mapTask.Type != MapTask {
		return errors.New("task type must be 'MapTask'")
	}

	conn, client, err := connectTo(node.Address)
	if err != nil {
		return err
	}
	defer (*conn).Close()
	defer client.Close()

	log.Printf("Sending task %v to '%v'...\n", mapTask.Id, node.Address)
	mapTaskStartRequest := data.StartMapTaskArgs{
		InputSplitFilePath: inputSplitFileName,
	}
	mapTaskStartReply := data.StartMapTaskReply{}
	err = client.Call(worker.StartMapTask, mapTaskStartRequest, &mapTaskStartReply)
	if err != nil {
		log.Printf("Could not receive %v reply from node: '%v'\n", worker.StartMapTask, err)
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

			for _, node := range state.Nodes {
				_, isNodeWithTaskAssigned := state.ReduceTasksAssignments[node.Address]
				if node.Status != NodeUp || isNodeWithTaskAssigned {
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

				err := runReduceTaskOnNode(node, reduceTask, intermediateFiles)
				if err != nil {
					state.AssignReduceTaskToNode(reduceTask.Id, node.Address)
				}
			}
		}
	}
}

func runReduceTaskOnNode(
	node Node,
	reduceTask Task,
	intermediateFiles []IntermediateFile,
) error {
	if reduceTask.Type != ReduceTask {
		return errors.New("task type must be 'ReduceTask'")
	}

	conn, client, err := connectTo(node.Address)
	if err != nil {
		return err
	}
	defer (*conn).Close()
	defer client.Close()

	log.Printf("Sending task %v to '%v'...\n", reduceTask.Id, node.Address)

	reduceIntermediateFiles := make([]data.ReduceIntermediateFile, len(intermediateFiles))
	for i, intermediateFile := range intermediateFiles {
		reduceIntermediateFiles[i] = data.ReduceIntermediateFile{
			Path:          intermediateFile.Path,
			WorkerAddress: node.Address,
		}
	}

	reduceTaskStartRequest := data.StartReduceTaskArgs{IntermediateFiles: reduceIntermediateFiles}
	reduceTaskStartReply := data.StartReduceTaskReply{}
	err = client.Call(worker.StartReduceTask, reduceTaskStartRequest, &reduceTaskStartReply)
	if err != nil {
		log.Printf("Could not receive %v reply from node: '%v'\n", worker.StartReduceTask, err)
		return err
	}

	return nil
}

func (masterServer *Server) FlagMapTaskAsCompleted(
	args data.CompletedMapTaskArgs,
	reply *data.CompletedMapTaskReply,
) error {
	state := masterServer.state
	intermediateFiles := make([]IntermediateFile, len(args.IntermediateFiles))

	for i, intermediateFile := range args.IntermediateFiles {
		intermediateFiles[i] = IntermediateFile{
			Path:      intermediateFile.Path,
			Partition: intermediateFile.Partition,
		}
	}

	state.CompleteMapTask(args.TaskId, args.WorkerAddress, intermediateFiles)
	return nil
}

func startMasterServer(serverAddress string, masterServer *Server) {
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
			log.Printf("Could not accept connection from node.\nReason: '%v'", err)
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
