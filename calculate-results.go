package main

import (
	"bufio"
	"bytes"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

type Location struct {
	name  string
	min   float64
	max   float64
	sum   float64
	count int
}

type JobDefinition struct {
	filename   string
	byteOffset int64
	byteLength int64
}

func toString(loc Location) string {
	return fmt.Sprintf("%s=%.1f/%.1f/%.1f", loc.name, loc.min, loc.sum/float64(loc.count), loc.max)
}

func parseFloat(byteStr []byte) float64 {
	value, _ := strconv.ParseFloat(string(byteStr), 32)
	return value
}

func processLine(line []byte, m map[string]*Location) {
	nameBytes, valueBytes, found := bytes.Cut(line, []byte{';'})
	if !found {
		fmt.Printf("Separator not found in line %s\n", line)
		return
	}

	name := string(nameBytes)
	value := parseFloat(valueBytes)
	oldEntry, exists := m[name]

	if exists {
		oldEntry.min = math.Min(oldEntry.min, value)
		oldEntry.max = math.Max(oldEntry.max, value)
		oldEntry.sum += value
		oldEntry.count++
	} else {
		m[name] = &Location{name, value, value, value, 1}
	}
}

func getFirstRune(line []byte) rune {
	for i := 0; i < len(line); i++ {
		rune, length := utf8.DecodeRune(line[i:])
		if length > 0 {
			return rune
		}
	}
	return '0'
}

func isValidLine(line []byte) bool {
	first := getFirstRune(line)
	return unicode.IsUpper(first) && unicode.IsLetter(first)
}

func processFilePart(ci <-chan JobDefinition, co chan<- map[string]*Location) {
	for job := range ci {
		fmt.Println("Starting to process ", job)

		readFile, err := os.Open(job.filename)
		if err != nil {
			fmt.Println(err)
		}

		if job.byteOffset > 0 {
			readFile.Seek(job.byteOffset-1, 0)
		}
		fileScanner := bufio.NewScanner(readFile)
		fileScanner.Split(bufio.ScanLines)

		m := make(map[string]*Location)

		bytesScanned := int64(0)
		for fileScanner.Scan() && bytesScanned < job.byteLength {
			line := fileScanner.Bytes()
			bytesScanned += int64(len(line))
			// All lines should start with an uppercase letter; discard those who don't.
			if isValidLine(line) {
				processLine(line, m)
			}
		}

		readFile.Close()

		co <- m
		fmt.Println("Completed processing ", job)
	}
}

func getFileSize(filename string) int64 {
	fi, err := os.Stat(filename)
	if err != nil {
		fmt.Println(err)
		return 0
	}

	return fi.Size()
}

func mergeLocations(a *Location, b *Location) *Location {
	return &Location{
		a.name,
		math.Min(a.min, b.min),
		math.Max(a.max, b.max),
		a.sum + b.sum,
		a.count + b.count,
	}
}

func mergeMaps(a map[string]*Location, b map[string]*Location) {
	for key, value := range b {
		oldValue, exists := a[key]
		if exists {
			a[key] = mergeLocations(oldValue, value)
		} else {
			a[key] = value
		}
	}
}

func main() {
	filename := os.Args[1]
	nWorkerThreads64, err := strconv.ParseInt(os.Args[2], 10, 8)
	nWorkerThreads := int(nWorkerThreads64)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fileSize := getFileSize(filename)
	blockSize := fileSize / int64(nWorkerThreads)

	c := make(chan JobDefinition)
	res := make(chan map[string]*Location)

	for i := 0; i < nWorkerThreads; i++ {
		go processFilePart(c, res)
	}

	for i := 0; i < nWorkerThreads; i++ {
		c <- JobDefinition{filename, int64(i) * blockSize, blockSize}
	}

	resultMap := make(map[string]*Location)

	for i := 0; i < nWorkerThreads; i++ {
		m := <-res
		mergeMaps(resultMap, m)
	}

	keys := make([]string, 0, len(resultMap))
	for k := range resultMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	results := make([]string, len(resultMap))
	for i, k := range keys {
		results[i] = toString(*resultMap[k])
	}
	fmt.Printf("{%s}\n", strings.Join(results, ", "))
}
