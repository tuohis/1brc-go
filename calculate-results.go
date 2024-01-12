package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
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

func processFilePart(filename string, byteOffset, byteLength int64, co chan<- LocationMap) {
	const BUFFER_SIZE = 1048576
	readFile, err := os.Open(filename)
	if err != nil {
		fmt.Println(err)
	}

	if byteOffset > 0 {
		readFile.Seek(byteOffset-1, 0)
	}
	fileScanner := bufio.NewScanner(readFile)
	fileScanner.Buffer(make([]byte, BUFFER_SIZE), BUFFER_SIZE)
	fileScanner.Split(bufio.ScanLines)

	m := make(LocationMap, INITIAL_MAP_SIZE)

	bytesScanned := int64(0)
	for fileScanner.Scan() && bytesScanned < byteLength {
		line := fileScanner.Bytes()
		bytesScanned += int64(len(line))
		// All lines should start with an uppercase letter; discard those who don't.
		if isValidLine(line) {
			processLine(line, m)
		}
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
