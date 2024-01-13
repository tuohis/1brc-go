# 1brc-go

One Billion Rows Challenge is here: https://github.com/gunnarmorling/1brc

That one is aimed at optimizing Java to the max, but I decided to try the challenge out with Go.
This repo is the result of that.

## Creating the Data File

As this is a spinoff of the `1brc` project, the same data file is used here. The data file is created using the following commands:

```
cd ..
git clone https://github.com/gunnarmorling/1brc.git
cd 1brc
./mvnw clean verify
./create_measurements.sh 1000000000
mv measurements.txt ../1brc-go/
```

## Running the Program

1. Run the program and measure execution time: `time go run . ./measurements.txt [num-of-parallel-threads]`

## Profiling the Program

1. Run the program with profiling via: `go test -cpuprofile cpu-8.prof -memprofile mem-8.prof -bench='BenchmarkEightThreads'`
1. Analyze the profile via: `go tool pprof cpu.8.prof`

## Results

The current result on a M1 Mac is ca. 10 seconds. There's still a lot of room for improvement, but in general it's a decent result.

