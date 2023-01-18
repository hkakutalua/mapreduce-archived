package data

type HeartbeatArgs struct{}
type HeartbeatReply struct {
	Err string
}

type StartMapTaskArgs struct {
	InputSplitFilePath string
}

type StartMapTaskReply struct {
	Err string
}

type ReduceIntermediateFile struct {
	Path          string
	WorkerAddress string
}

type StartReduceTaskArgs struct {
	IntermediateFiles []ReduceIntermediateFile
}

type StartReduceTaskReply struct {
	Err string
}

type MapIntermediateFile struct {
	Path      string
	Partition int
}

type CompletedMapTaskArgs struct {
	TaskId            int
	WorkerAddress     string
	IntermediateFiles []MapIntermediateFile
}

type CompletedMapTaskReply struct {
	Error string
}
