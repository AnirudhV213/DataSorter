package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/AnirudhV16/DataSorter/consumer"
	"github.com/AnirudhV16/DataSorter/data"
	"github.com/AnirudhV16/DataSorter/producer"
	"github.com/AnirudhV16/DataSorter/verify"
)

func main() {
	// ── CLI flags ─────────────────────────────────────────────────────────
	brokerList := flag.String("brokers", "localhost:9092",
		"comma-separated Kafka broker addresses")
	csvPath := flag.String("csv", "data.csv",
		"path for the generated CSV file")
	count := flag.Int("count", 50_000_000,
		"number of records to generate")
	skipGen := flag.Bool("skip-gen", false,
		"skip CSV generation — use existing file")
	skipProduce := flag.Bool("skip-produce", false,
		"skip producing to Kafka — run sorters only")
	flag.Parse()

	brokers := strings.Split(*brokerList, ",")

	// Graceful shutdown on Ctrl-C / SIGTERM.
	ctx, cancel := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer cancel()

	wall := time.Now()

	// ── Step 1: generate CSV ──────────────────────────────────────────────
	if !*skipGen {
		fmt.Printf("═══ Step 1: Generating %d records → %s ═══\n", *count, *csvPath)
		t0 := time.Now()
		if err := data.GenerateToCSV(*csvPath, *count); err != nil {
			fatalf("generation failed: %v", err)
		}
		fmt.Printf("Generation done in %.2fs\n\n", time.Since(t0).Seconds())
	}

	// ── Step 2: produce CSV → Kafka source topic ──────────────────────────
	if !*skipProduce {
		fmt.Printf("═══ Step 2: Producing %s → Kafka topic %q ═══\n",
			*csvPath, producer.TopicSource)
		t0 := time.Now()
		if err := producer.SendCSVToKafka(ctx, brokers, *csvPath); err != nil {
			fatalf("kafka producer failed: %v", err)
		}
		fmt.Printf("Produce done in %.2fs\n\n", time.Since(t0).Seconds())
	}

	// ── Step 3: sorter consumers ──────────────────────────────────────────
	fmt.Printf("═══ Step 3: Running sorters (id / name / continent) ═══\n")
	if err := consumer.RunAllSorters(ctx, brokers); err != nil {
		fatalf("sorters failed: %v", err)
	}

	fmt.Printf("\n✓ Full pipeline complete. Total wall-clock: %.2fs\n",
		time.Since(wall).Seconds())

	// step4: ------------------------------------------------
	//verification
	fmt.Println("verification of sorted data")
	//tx context.Context, brokers []string, outputDir string
	verify.VerifyAll(context.Background(), brokers, "/output_csv")
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "FATAL: "+format+"\n", args...)
	os.Exit(1)
}
