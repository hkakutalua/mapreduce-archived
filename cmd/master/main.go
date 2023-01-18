package main

import (
	"log"
	"os"
	"strconv"

	"github.com/henrick/mapreduce/pkg/master"
)

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

	master.Run(m, r, inputFileName, workerAddresses)
}
