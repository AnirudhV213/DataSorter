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
)

func main() {
	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		brokersEnv = "localhost:9092"
	}

	brokerList := flag.String("brokers", brokersEnv, "comma-separated Kafka broker addresses")
	csvPath := flag.String("csv", "data.csv", "path for the generated CSV file")
	count := flag.Int("count", 50_000_000, "number of records to generate")
	skipGen := flag.Bool("skip-gen", false, "skip CSV generation — use existing file")
	skipProduce := flag.Bool("skip-produce", false, "skip producing to Kafka — run sorters only")
	flag.Parse()

	brokers := strings.Split(*brokerList, ",")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	wall := time.Now()

	var stepGenDur, stepProduceDur, stepSortDur time.Duration

	// ── Step 1: generate CSV ──────────────────────────────────────────────
	if !*skipGen {
		fmt.Printf("═══ Step 1: Generating %d records → %s ═══\n", *count, *csvPath)
		t0 := time.Now()
		if err := data.GenerateToCSV(*csvPath, *count); err != nil {
			fatalf("generation failed: %v", err)
		}
		stepGenDur = time.Since(t0)
		fmt.Printf("Generation done in %.2fs\n\n", stepGenDur.Seconds())
	}

	// ── Step 2: produce CSV → Kafka source topic ──────────────────────────
	if !*skipProduce {
		fmt.Printf("═══ Step 2: Producing %s → Kafka topic %q ═══\n", *csvPath, producer.TopicSource)
		t0 := time.Now()
		if err := producer.SendCSVToKafka(ctx, brokers, *csvPath); err != nil {
			fatalf("kafka producer failed: %v", err)
		}
		stepProduceDur = time.Since(t0)
		fmt.Printf("Produce done in %.2fs\n\n", stepProduceDur.Seconds())
	}

	// ── Step 3: sorter consumers ──────────────────────────────────────────
	fmt.Printf("═══ Step 3: Running sorters (id / name / continent) ═══\n")
	t0 := time.Now()
	if err := consumer.RunAllSorters(ctx, brokers); err != nil {
		fatalf("sorters failed: %v", err)
	}
	stepSortDur = time.Since(t0)

	totalDur := time.Since(wall)

	// ── Summary ───────────────────────────────────────────────────────────
	fmt.Println()
	fmt.Println("══════════════════════════════════════════")
	fmt.Println("             Pipeline Summary             ")
	fmt.Println("══════════════════════════════════════════")
	if !*skipGen {
		fmt.Printf("  Step 1 — CSV generation:   %8.2fs\n", stepGenDur.Seconds())
	} else {
		fmt.Printf("  Step 1 — CSV generation:   (skipped)\n")
	}
	if !*skipProduce {
		fmt.Printf("  Step 2 — Kafka produce:    %8.2fs\n", stepProduceDur.Seconds())
	} else {
		fmt.Printf("  Step 2 — Kafka produce:    (skipped)\n")
	}
	fmt.Printf("  Step 3 — Sort & publish:   %8.2fs\n", stepSortDur.Seconds())
	fmt.Println("  ──────────────────────────────────────")
	fmt.Printf("  Total wall-clock:          %8.2fs\n", totalDur.Seconds())
	fmt.Println("══════════════════════════════════════════")
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "FATAL: "+format+"\n", args...)
	os.Exit(1)
}
