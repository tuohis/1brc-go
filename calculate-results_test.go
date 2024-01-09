package main

import (
	"testing"
)

func BenchmarkSingleThread(b *testing.B) {
	for i := 0; i < b.N; i++ {
		parseFile("../1brc/measurements.txt", 1)
	}
}

func BenchmarkTwoThreads(b *testing.B) {
	for i := 0; i < b.N; i++ {
		parseFile("../1brc/measurements.txt", 2)
	}
}

func BenchmarkFourThreads(b *testing.B) {
	for i := 0; i < b.N; i++ {
		parseFile("../1brc/measurements.txt", 4)
	}
}

func BenchmarkEightThreads(b *testing.B) {
	for i := 0; i < b.N; i++ {
		parseFile("../1brc/measurements.txt", 8)
	}
}
