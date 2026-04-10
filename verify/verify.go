package verify

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/AnirudhV16/DataSorter/consumer"
	"github.com/AnirudhV16/DataSorter/producer"
	"github.com/IBM/sarama"
)

// ─── Verifier ─────────────────────────────────────────────────────────────────

// VerifyAll reads the three sorted output topics (id, name, continent) and
// writes each one to a CSV file in outputDir.
//
// Output files:
//   - outputDir/sorted_by_id.csv
//   - outputDir/sorted_by_name.csv
//   - outputDir/sorted_by_continent.csv
//
// Each file has the same header as the source CSV:
//
//	id,name,address,continent
//
// How it works:
//
//	Each output topic has exactly 1 partition, so records are stored in the
//	exact order the sorter wrote them. We read from offset 0 to HWM-1 using
//	a direct partition consumer — the same approach as consumeAll in sorter.go
//	— and stream each message straight to the file writer without buffering
//	the full dataset in memory.
func VerifyAll(ctx context.Context, brokers []string, outputDir string) error {
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return fmt.Errorf("create output dir %q: %w", outputDir, err)
	}

	topics := []struct {
		sortKey  consumer.SortKey
		filename string
	}{
		{consumer.SortByID, "sorted_by_id.csv"},
		{consumer.SortByName, "sorted_by_name.csv"},
		{consumer.SortByContinent, "sorted_by_continent.csv"},
	}

	for _, t := range topics {
		outPath := filepath.Join(outputDir, t.filename)
		fmt.Printf("[verify] reading topic=%q → %s\n", t.sortKey.OutputTopic(), outPath)

		start := time.Now()
		count, err := dumpTopicToCSV(ctx, brokers, t.sortKey.OutputTopic(), outPath)
		if err != nil {
			return fmt.Errorf("[verify] topic %q: %w", t.sortKey, err)
		}

		fmt.Printf("[verify] wrote %d records to %s in %.2fs\n",
			count, outPath, time.Since(start).Seconds())
	}

	return nil
}

// dumpTopicToCSV reads every message from a single-partition topic and writes
// them line-by-line to a CSV file. Returns the number of records written.
//
// Streaming approach: messages are written to disk as they arrive — no
// in-memory accumulation — so memory usage is O(1) regardless of topic size.
func dumpTopicToCSV(ctx context.Context, brokers []string, topic, outPath string) (int64, error) {
	// ── 1. Open Sarama client and query partition bounds ──────────────────
	client, err := sarama.NewClient(brokers, consumer.NewClientConfig())
	if err != nil {
		return 0, fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	// Output topics have exactly 1 partition (partition 0).
	const partitionID = int32(0)

	oldest, err := client.GetOffset(topic, partitionID, sarama.OffsetOldest)
	if err != nil {
		return 0, fmt.Errorf("get oldest offset: %w", err)
	}
	hwm, err := client.GetOffset(topic, partitionID, sarama.OffsetNewest)
	if err != nil {
		return 0, fmt.Errorf("get HWM: %w", err)
	}

	messageCount := hwm - oldest
	fmt.Printf("[verify]   topic=%q  oldest=%d  HWM=%d  count=%d\n",
		topic, oldest, hwm, messageCount)

	if messageCount == 0 {
		return 0, fmt.Errorf("topic %q is empty — sorter may not have run yet", topic)
	}

	// ── 2. Open the output CSV file ───────────────────────────────────────
	f, err := os.Create(outPath)
	if err != nil {
		return 0, fmt.Errorf("create file %q: %w", outPath, err)
	}
	defer f.Close()

	// 4 MB write buffer — same rationale as the generator.
	const writeBufSize = 4 * 1024 * 1024
	w := bufio.NewWriterSize(f, writeBufSize)

	// Write the CSV header.
	if _, err := fmt.Fprintln(w, "id,name,address,continent"); err != nil {
		return 0, fmt.Errorf("write header: %w", err)
	}

	// ── 3. Open the partition consumer ────────────────────────────────────
	directConsumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return 0, fmt.Errorf("new consumer: %w", err)
	}
	defer directConsumer.Close()

	pc, err := directConsumer.ConsumePartition(topic, partitionID, sarama.OffsetOldest)
	if err != nil {
		return 0, fmt.Errorf("consume partition: %w", err)
	}
	defer pc.Close()

	// ── 4. Stream messages → CSV ──────────────────────────────────────────
	var written int64
	const logInterval = 5_000_000

	for {
		select {
		case <-ctx.Done():
			return written, ctx.Err()

		case msg, ok := <-pc.Messages():
			if !ok {
				goto flush
			}

			// Validate the line parses correctly before writing.
			if _, err := producer.ParseCSVLine(string(msg.Value)); err != nil {
				fmt.Fprintf(os.Stderr,
					"[verify] bad record at offset %d: %v\n", msg.Offset, err)
			} else {
				// Write the raw CSV line directly — no re-serialisation needed.
				if _, err := fmt.Fprintln(w, string(msg.Value)); err != nil {
					return written, fmt.Errorf("write record offset %d: %w", msg.Offset, err)
				}
				written++
			}

			if written%logInterval == 0 {
				fmt.Printf("[verify]   %s: wrote %d / %d records\n",
					topic, written, messageCount)
			}

			// Stop exactly at HWM-1 (last message that existed at query time).
			if msg.Offset >= hwm-1 {
				goto flush
			}
		}
	}

flush:
	if err := w.Flush(); err != nil {
		return written, fmt.Errorf("flush writer: %w", err)
	}

	return written, nil
}
