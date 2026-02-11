package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
)

type eventObject struct {
	// Event.value is optional; only entries with this field contribute to the map.
	EventValue *int `json:"Event.value"`
}

func main() {
	var filePath string
	var maxValue int
	var printMissing bool

	flag.StringVar(&filePath, "file", "", "Path to the JSON file to scan")
	flag.IntVar(&maxValue, "max", 100005, "Maximum event value (exclusive) to check for missing values")
	flag.BoolVar(&printMissing, "print-missing", true, "Print each missing value")
	flag.Parse()

	if filePath == "" {
		fmt.Fprintln(os.Stderr, "error: -file is required")
		os.Exit(1)
	}
	if maxValue <= 0 {
		fmt.Fprintln(os.Stderr, "error: -max must be > 0")
		os.Exit(1)
	}

	file, err := os.Open(filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: open file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	decoder := json.NewDecoder(reader)

	// Stream the top-level array so large files don't need to fit in memory.
	tok, err := decoder.Token()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: read JSON start token: %v\n", err)
		os.Exit(1)
	}
	delim, ok := tok.(json.Delim)
	if !ok || delim != '[' {
		fmt.Fprintln(os.Stderr, "error: expected JSON array at top level")
		os.Exit(1)
	}

	// seen[i] == true means Event.value == i was present in the file.
	seen := make([]bool, maxValue)
	totalValues := 0

	for decoder.More() {
		var obj eventObject
		if err := decoder.Decode(&obj); err != nil {
			fmt.Fprintf(os.Stderr, "error: decode object: %v\n", err)
			os.Exit(1)
		}

		if obj.EventValue != nil {
			value := *obj.EventValue
			if value >= 0 && value < maxValue {
				if !seen[value] {
					seen[value] = true
					totalValues++
				}
			}
		}
	}

	// Consume the closing array token if present.
	if _, err := decoder.Token(); err != nil {
		if err != io.EOF {
			fmt.Fprintf(os.Stderr, "error: read JSON end token: %v\n", err)
			os.Exit(1)
		}
	}

	missing := 0
	for i, ok := range seen {
		if !ok {
			missing++
			if printMissing {
				fmt.Println(i)
			}
		}
	}

	fmt.Printf("checked=0..%d seen=%d missing=%d\n", maxValue-1, totalValues, missing)
}
