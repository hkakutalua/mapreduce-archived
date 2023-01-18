package worker

import (
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"

	"github.com/henrick/mapreduce/pkg/data"
)

const GetHeartbeat = "Server.GetHeartbeat"
const StartMapTask = "Server.StartMapTask"
const StartReduceTask = "Server.StartReduceTask"

type Server struct{}

func Run() {
	port := 12345
	log.Printf("Listening to connections on port %v\n", port)

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		log.Fatalf("Could not listen to TCP connections on port :%v: %v", port, err)
		os.Exit(-1)
	}
	defer listener.Close()

	server := new(Server)
	err = rpc.Register(server)
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

func (server *Server) GetHeartbeat(args *data.HeartbeatArgs, reply *data.HeartbeatReply) error {
	*reply = data.HeartbeatReply{}
	return nil
}

func (server *Server) StartMapTask(args *data.StartMapTaskArgs, reply *data.StartMapTaskReply) error {
	*reply = data.StartMapTaskReply{}
	return nil
}
