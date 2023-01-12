package main

import (
	"distributedsystems/mapreduce"
)

func main() {
	files := []string{"CSV_Data_2022_11_17 2 44.csv"}

	nodes := []mapreduce.Node{
		mapreduce.Node{Address: "127.0.0.1", User: "henrick", Type: "Master"},
		mapreduce.Node{Address: "127.0.0.1", User: "henrick", Type: "Worker"},
		mapreduce.Node{Address: "127.0.0.1", User: "henrick", Type: "Worker"},
		mapreduce.Node{Address: "127.0.0.1", User: "henrick", Type: "Worker"},
	}
	mapSpec := mapreduce.MapSpecification{FilePaths: files, M: 4}
	reduceSpec := mapreduce.ReduceSpecification{OutputFile: "out", R: 4}

	mapreduce.Start(nodes, mapSpec, reduceSpec, "wordcount.so")
}
