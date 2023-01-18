package master

import "sync"

type TaskStatus int

const (
	Idle       = 1
	InProgress = 2
	Completed  = 3
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
	NodeDown = 1
	NodeUp   = 2
)

type Node struct {
	Address string
	Status  Status
}

type State struct {
	M                      int
	R                      int
	InputFilePath          string
	MapTasks               []Task
	ReduceTasks            []Task
	Nodes                  []Node
	MapTasksAssignments    map[string]int                // NodeAddress -> TaskId
	ReduceTasksAssignments map[string]int                // NodeAddress -> TaskId
	IntermediateFiles      map[string][]IntermediateFile // NodeAddress -> []IntermediateFile
	mutex                  sync.Mutex
}

func (state *State) UpdateNodeStatus(nodeAddress string, newStatus Status) {
	state.mutex.Lock()
	defer state.mutex.Unlock()

	for i, node := range state.Nodes {
		if node.Address == nodeAddress {
			state.Nodes[i].Status = newStatus
		}
	}
}

func (state *State) AssignMapTaskToNode(taskId int, nodeAddress string) {
	state.mutex.Lock()
	defer state.mutex.Unlock()

	for i, mapTask := range state.MapTasks {
		if mapTask.Id == taskId {
			state.MapTasksAssignments[nodeAddress] = taskId
			mapTask.Status = InProgress
			state.MapTasks[i] = mapTask
			break
		}
	}
}

func (state *State) AssignReduceTaskToNode(taskId int, nodeAddress string) {
	state.mutex.Lock()
	defer state.mutex.Unlock()

	for i, reduceTask := range state.ReduceTasks {
		if reduceTask.Id == taskId {
			state.ReduceTasksAssignments[nodeAddress] = taskId
			reduceTask.Status = InProgress
			state.ReduceTasks[i] = reduceTask
			break
		}
	}
}

func (state *State) ResetTaskOnNode(nodeAddress string) {
	state.mutex.Lock()
	defer state.mutex.Unlock()

	mapTaskId, hasMapTask := state.MapTasksAssignments[nodeAddress]
	if hasMapTask {
		delete(state.MapTasksAssignments, nodeAddress)
		mapTask := state.MapTasks[mapTaskId]
		mapTask.Status = Idle
		state.IntermediateFiles[nodeAddress] = nil
		state.MapTasks[mapTaskId] = mapTask
	}

	reduceTaskId, hasReduceTask := state.ReduceTasksAssignments[nodeAddress]
	if hasReduceTask {
		delete(state.ReduceTasksAssignments, nodeAddress)
		reduceTask := state.ReduceTasks[reduceTaskId]
		if reduceTask.Status != Completed {
			reduceTask.Status = Idle
			state.MapTasks[reduceTaskId] = reduceTask
		}
	}
}

func (state *State) CompleteMapTask(
	taskId int,
	nodeAddress string,
	intermediateFiles []IntermediateFile,
) {
	state.mutex.Lock()
	defer state.mutex.Unlock()

	mapTask := state.MapTasks[taskId]
	mapTask.Status = Completed
	state.MapTasks[taskId] = mapTask

	newIntermediateFiles := append(state.IntermediateFiles[nodeAddress], intermediateFiles...)
	state.IntermediateFiles[nodeAddress] = newIntermediateFiles
}
