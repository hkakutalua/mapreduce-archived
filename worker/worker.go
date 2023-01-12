package main

import (
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
)

type WorkerHeartbeat bool
type HeartbeatRequest struct{}
type HeartbeatReply struct{}

func (w *WorkerHeartbeat) GetHeartbeat(args *HeartbeatRequest, reply *HeartbeatReply) error {
	*reply = HeartbeatReply{}
	return nil
}

type WorkerTask struct{}
type MapTaskStartRequest struct {
	InputSplitFileName string
	M                  int
}
type MapTaskStartReply struct{}

func (w *WorkerTask) MapTaskStart(args *MapTaskStartRequest, reply *MapTaskStartReply) error {
	*reply = MapTaskStartReply{}
	return nil
}

func main() {
	port := 12345
	log.Printf("Listening to connections on port %v\n", port)

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		log.Fatalf("Could not listen to TCP connections on port :%v: %v", port, err)
		os.Exit(-1)
	}
	defer listener.Close()

	workerHeartbeat := new(WorkerHeartbeat)
	err = rpc.Register(workerHeartbeat)
	if err != nil {
		log.Fatalf("Could not register RPC functions: %v", err)
		os.Exit(-1)
	}

	workerTask := new(WorkerTask)
	err = rpc.Register(workerTask)
	if err != nil {
		log.Fatalf("Could not register RPC functions: %v", err)
		os.Exit(-1)
	}

	log.Printf("Accepting connections on port %v\n", port)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatalf("Could not accept a TCP connection on port :%v: %v", port, err)
			os.Exit(-1)
		}

		go func(conn net.Conn) {
			log.Printf("Accepted connection from host %v\n", conn.RemoteAddr())
			rpc.ServeConn(conn)
			log.Printf("Closing connection from host %v\n", conn.RemoteAddr())
		}(conn)
	}
}
