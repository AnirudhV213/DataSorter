/*
package main

import "github.com/AnirudhV16/DataSorter/data"

	func main() {
		data.GenerateToCSV("C:/Users/AnirudhReddy/DataSorter/producer/data/dummy.csv", 5000000)
	}
*/
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/AnirudhV16/DataSorter/data"
	"github.com/AnirudhV16/DataSorter/producer"
)

func main() {
	// ── flags ──────────────────────────────────────────────────────────────
	brokerList := flag.String("brokers", "localhost:9092",
		"comma-separated Kafka broker addresses")
	csvPath := flag.String("csv", "data.csv",
		"path to the generated CSV file")
	count := flag.Int("count", 50_000_000,
		"number of records to generate (skipped if -skip-gen is set)")
	skipGen := flag.Bool("skip-gen", false,
		"skip CSV generation and use an existing file at -csv")
	flag.Parse()

	brokers := strings.Split(*brokerList, ",")

	// ── step 1: generate CSV (optional) ────────────────────────────────────
	if !*skipGen {
		fmt.Printf("=== Step 1: Generating %d records → %s ===\n", *count, *csvPath)
		t0 := time.Now()
		if err := data.GenerateToCSV(*csvPath, *count); err != nil {
			fmt.Fprintf(os.Stderr, "generation failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Generation complete in %.2fs\n\n", time.Since(t0).Seconds())
	} else {
		fmt.Printf("Skipping generation; using existing file %q\n\n", *csvPath)
	}

	// ── step 2: send CSV → Kafka ────────────────────────────────────────────
	fmt.Printf("=== Step 2: Sending %s → Kafka topic %q ===\n",
		*csvPath, producer.TopicSource)

	wall := time.Now()
	if err := producer.SendCSVToKafka(context.Background(), brokers, *csvPath); err != nil {
		fmt.Fprintf(os.Stderr, "kafka producer failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\nTotal wall-clock time (gen + produce): %.2fs\n", time.Since(wall).Seconds())
}
