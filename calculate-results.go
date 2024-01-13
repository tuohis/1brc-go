//  Copyright 2024 Mikko Tuohimaa
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

type TupleIntString struct {
	num int
	str string
}

// Measurement is a fixed precision decimal with 0.1 accuracy
type Measurement int64

func (m Measurement) toFloat() float64 {
	return float64(m) / 10
}

func parseMeasurement(byteStr []byte) Measurement {
	zeroCode := byte('0')

	// The value has exactly one decimal
	multiplier := int64(1)
	if byteStr[0] == '-' {
		multiplier = -1
	}

	intValue := int64(0)
	for _, b := range byteStr {
		if b >= '0' && b <= '9' {
			intValue = intValue*10 + int64(b-zeroCode)
		}
	}
	return Measurement(multiplier * intValue)
}

func min(a, b Measurement) Measurement {
	if a < b {
		return a
	}
	return b
}

func max(a, b Measurement) Measurement {
	if a < b {
		return b
	}
	return a
}

type Location struct {
	name  []byte
	hash  int
	min   Measurement
	max   Measurement
	sum   Measurement
	count int
}

func (loc *Location) toString() string {
	return fmt.Sprintf("%s=%.1f/%.1f/%.1f", loc.name, loc.min.toFloat(), loc.sum.toFloat()/float64(loc.count), loc.max.toFloat())
}

func (a *Location) merge(b *Location) *Location {
	return &Location{
		a.name,
		a.hash,
		min(a.min, b.min),
		max(a.max, b.max),
		a.sum + b.sum,
		a.count + b.count,
	}
}

func (loc *Location) append(m Measurement) {
	loc.min = min(loc.min, m)
	loc.max = max(loc.max, m)
	loc.sum += m
	loc.count++
}

type LocationMap map[int]*Location

func (a LocationMap) merge(b LocationMap) LocationMap {
	for key, loc := range b {
		oldLocation, exists := a[key]
		if exists {
			a[key] = oldLocation.merge(loc)
		} else {
			a[key] = loc
		}
	}
	return a
}

type JobDefinition struct {
	filename   string
	byteOffset int64
	byteLength int64
}

const INITIAL_MAP_SIZE = 2048

func calculateHash(bytes []byte) int {
	h := 0x811c9dc5
	for _, b := range bytes {
		h = (h ^ int(b)) * 0x01000193
	}
	return h
}

func processLine(line []byte, m LocationMap) {
	nameBytes, valueBytes, found := bytes.Cut(line, []byte{';'})
	if !found {
		fmt.Printf("Separator not found in line %s\n", line)
		return
	}

	hash := calculateHash(nameBytes)
	value := parseMeasurement(valueBytes)
	oldEntry, exists := m[hash]

	if exists {
		oldEntry.append(value)
	} else {
		name := make([]byte, len(nameBytes))
		copy(name, nameBytes)
		// name := nameBytes
		m[hash] = &Location{name, hash, value, value, value, 1}
	}
}

func seekToLineStart(readFile *os.File, byteOffset int64) (int64, error) {
	if byteOffset == 0 {
		// Start of file is also a start of line
		return 0, nil
	}

	readBuffer := make([]byte, 128)

	// Set the read pointer to the byte that's the first to follow a '\n' and be at or after byteOffset
	readFile.Seek(byteOffset-1, 0)
	for byteIndex := int64(0); byteIndex < 1024; {
		bytesRead, err := readFile.Read(readBuffer)
		if err != nil {
			fmt.Println(err)
			return byteIndex, err
		}

		for i, b := range readBuffer[0:bytesRead] {
			if b == '\n' {
				position := byteOffset + byteIndex + int64(i)
				readFile.Seek(position, 0)
				return position - byteOffset, nil
			}
		}
		byteIndex += int64(bytesRead)
	}
	return 1024, errors.New("No newline found!")
}

func processFilePart(filename string, byteOffset, byteLength int64, co chan<- LocationMap) {
	readFile, err := os.Open(filename)
	if err != nil {
		fmt.Println(err)
		return
	}

	const BUFFER_SIZE = 1048576
	readBuffer := make([]byte, BUFFER_SIZE)
	bytesScanned := int64(0)

	if byteOffset > 0 {
		bytesDiscarded, err := seekToLineStart(readFile, byteOffset)
		if err != nil {
			fmt.Println("Error when seeking newline: ", err)
			return
		}
		bytesScanned += bytesDiscarded
	}

	fileScanner := bufio.NewScanner(readFile)
	fileScanner.Buffer(readBuffer, BUFFER_SIZE)
	fileScanner.Split(bufio.ScanLines)

	m := make(LocationMap, INITIAL_MAP_SIZE)

	for fileScanner.Scan() && bytesScanned < byteLength {
		line := fileScanner.Bytes()
		bytesScanned += int64(len(line))
		processLine(line, m)
	}

	readFile.Close()

	co <- m
}

func getFileSize(filename string) int64 {
	fi, err := os.Stat(filename)
	if err != nil {
		fmt.Println(err)
		return 0
	}

	return fi.Size()
}

func parseFile(filename string, nWorkerThreads int) LocationMap {
	fileSize := getFileSize(filename)
	blockSize := fileSize / int64(nWorkerThreads)

	res := make(chan LocationMap)

	for i := 0; i < nWorkerThreads; i++ {
		go processFilePart(filename, int64(i)*blockSize, blockSize, res)
	}

	resultMap := make(LocationMap, INITIAL_MAP_SIZE)

	for i := 0; i < nWorkerThreads; i++ {
		m := <-res
		resultMap.merge(m)
	}

	return resultMap
}

func printResults(resultMap LocationMap) {
	keys := make([]TupleIntString, 0, len(resultMap))
	for key, value := range resultMap {
		keys = append(keys, TupleIntString{key, string(value.name)})
	}

	sort.Slice(keys, func(i, j int) bool { return keys[i].str < keys[j].str })
	fmt.Println("Total locations:", len(keys))

	results := make([]string, len(resultMap))
	for i, item := range keys {
		results[i] = resultMap[item.num].toString()
	}
	fmt.Printf("{%s}\n", strings.Join(results, ", "))
}

func main() {
	filename := "../1brc/measurements.txt"
	nWorkerThreads := 1

	if len(os.Args) > 2 {
		nWorkerThreads64, err := strconv.ParseInt(os.Args[2], 10, 8)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		nWorkerThreads = int(nWorkerThreads64)
	}
	if len(os.Args) > 1 {
		filename = os.Args[1]
	}

	resultMap := parseFile(filename, nWorkerThreads)
	printResults(resultMap)

}
