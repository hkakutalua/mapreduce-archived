package main

import "distributedsystems/mapreduce"

func Map(key string, value string) mapreduce.MapResult {
	return mapreduce.MapResult{IntermediateKey: "1", Value: "1"}
}

func Reduce(intermediateKey string, values []string) []string {
	return []string{}
}
