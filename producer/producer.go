package producer

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AnirudhV16/DataSorter/data"
	"github.com/IBM/sarama"
)

// ─── Kafka configuration ────────────────────────────────────────────────────

const (
	// TopicSource is the Kafka topic all generated records are sent to.
	TopicSource = "source"

	// NumPartitions is the partition count for the source topic.
	// Three partitions allow three parallel consumer groups to read concurrently.
	NumPartitions = 3

	// workerCount controls how many goroutines feed the Kafka async producer.
	// Saturating the producer with more goroutines than CPU cores gives
	// diminishing returns; 8 is a good default for a 4-core machine.
	workerCount = 4

	// batchSize is the number of CSV lines each worker accumulates before
	// sending to the async producer.  Larger batches reduce channel overhead.
	batchSize = 500

	// channelBuffer is the depth of the line-distribution channel.
	// Deep enough to keep workers fed even when the reader briefly stalls.
	channelBuffer = 5000

	// progressInterval — print a progress line every N messages sent.
	progressInterval = 5_000_000
)

// ─── Topic admin ────────────────────────────────────────────────────────────

// EnsureTopic creates the topic if it does not already exist.
// It is idempotent: calling it on an already-existing topic is a no-op.
func EnsureTopic(brokers []string, topic string, partitions int32) error {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_6_0_0

	admin, err := sarama.NewClusterAdmin(brokers, cfg)
	if err != nil {
		return fmt.Errorf("cluster admin: %w", err)
	}
	defer admin.Close()

	detail := &sarama.TopicDetail{
		NumPartitions:     partitions,
		ReplicationFactor: 1, // single-broker dev setup
	}

	err = admin.CreateTopic(topic, detail, false)
	if err != nil {
		// ErrTopicAlreadyExists is not a real error for us.
		if kafkaErr, ok := err.(*sarama.TopicError); ok &&
			kafkaErr.Err == sarama.ErrTopicAlreadyExists {
			return nil
		}
		return fmt.Errorf("create topic %q: %w", topic, err)
	}

	fmt.Printf("Topic %q created with %d partitions\n", topic, partitions)
	return nil
}

// newAdmin builds a Sarama ClusterAdmin.
func newAdmin(brokers []string) (sarama.ClusterAdmin, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_6_0_0
	admin, err := sarama.NewClusterAdmin(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("cluster admin: %w", err)
	}
	return admin, nil
}

// RecreateTopic deletes the topic if it exists, waits for async deletion,
// then creates it fresh — guaranteeing HWM=0 on every run.
// RecreateTopic deletes the topic if it exists, waits for async deletion,
// then creates it fresh — guaranteeing HWM=0 on every run.
func RecreateTopic(brokers []string, topic string, partitions int32) error {
	admin, err := newAdmin(brokers)
	if err != nil {
		return err
	}
	defer admin.Close()

	if err := admin.DeleteTopic(topic); err != nil {
		// "topic does not exist" is fine — it just means first run.
		// Any other error is a real problem.
		kafkaErr, ok := err.(*sarama.TopicError)
		if !ok || kafkaErr.Err != sarama.ErrUnknownTopicOrPartition {
			return fmt.Errorf("delete topic %q: %w", topic, err)
		}
		// Topic didn't exist — skip the sleep, fall through to create.
	} else {
		fmt.Printf("Topic %q deleted (clearing stale data)\n", topic)
		// Broker deletion is async — wait before recreating to avoid
		// ErrTopicAlreadyExists being returned immediately after delete.
		time.Sleep(2 * time.Second)
	}

	detail := &sarama.TopicDetail{
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	}
	if err := admin.CreateTopic(topic, detail, false); err != nil {
		return fmt.Errorf("recreate topic %q: %w", topic, err)
	}
	fmt.Printf("Topic %q created fresh with %d partitions\n", topic, partitions)
	return nil
}

// ─── Producer config ────────────────────────────────────────────────────────

// newAsyncProducerConfig returns a Sarama config tuned for high throughput
// within the 2 GB / 4-core budget.
//
// Key tuning choices:
//   - WaitForAll acks: every message is confirmed by the broker leader before
//     being considered sent — keeps data durable without full ISR sync.
//   - Snappy compression: ~2× throughput vs no compression for text CSV data
//     because we become I/O-bound on the broker side; Snappy has low CPU cost.
//   - Large batch + linger: Sarama flushes when either Frequency or Messages
//     threshold is hit; 100 ms linger lets batches fill up before a flush,
//     dramatically reducing the number of produce requests.
//   - Channel buffer of 1 M: prevents the async producer from blocking our
//     workers when the broker is briefly slow.
func newAsyncProducerConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_6_0_0

	// Acknowledgements
	cfg.Producer.RequiredAcks = sarama.WaitForAll

	// Compression — Snappy offers the best throughput/CPU trade-off for text
	cfg.Producer.Compression = sarama.CompressionSnappy

	// Batching — flush when 64 K messages accumulated OR 100 ms elapsed
	cfg.Producer.Flush.Messages = 5000
	cfg.Producer.Flush.Frequency = 100 * time.Millisecond

	// Retry on transient errors
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Retry.Backoff = 200 * time.Millisecond

	// Deep channel so workers are never blocked waiting for the producer
	cfg.ChannelBufferSize = 10_000

	// Return successes and errors so we can count them and drain the channels
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true

	return cfg
}

// ─── Partition router ───────────────────────────────────────────────────────

// roundRobinPartitioner cycles through partitions sequentially.
// Round-robin gives perfectly uniform distribution across the three partitions,
// maximising parallelism on the consumer side.
type roundRobinPartitioner struct {
	counter uint64 // atomic; no lock needed
}

func (r *roundRobinPartitioner) Partition(msg *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	idx := atomic.AddUint64(&r.counter, 1) - 1
	return int32(idx % uint64(numPartitions)), nil
}

func (r *roundRobinPartitioner) RequiresConsistency() bool { return false }

// newRoundRobinPartitioner is the Sarama PartitionerConstructor signature.
func newRoundRobinPartitioner(_ string) sarama.Partitioner {
	return &roundRobinPartitioner{}
}

// ─── CSV → Kafka pipeline ───────────────────────────────────────────────────

// SendCSVToKafka reads csvPath line-by-line and sends every data row (skipping
// the header) to the Kafka `source` topic.
//
// Architecture:
//
//	┌─────────┐   lines chan   ┌──────────────────────┐   ProducerMessage
//	│  reader │──────────────▶│  workerCount workers  │──────────────────▶ Kafka
//	│ goroutine│               └──────────────────────┘
//	└─────────┘
//
// The reader goroutine is I/O-bound; the workers are CPU-bound (string parsing
// + message construction). Separating them via a buffered channel keeps both
// saturated.
//
// The error/success drainer goroutine prevents the async producer's internal
// channels from filling up, which would block the workers.
// topicExists returns true if the topic is already present on the broker.
func topicExists(brokers []string, topic string) bool {
	admin, err := newAdmin(brokers)
	if err != nil {
		return false
	}
	defer admin.Close()

	topics, err := admin.ListTopics()
	if err != nil {
		return false
	}

	_, exists := topics[topic]
	return exists
}

func SendCSVToKafka(ctx context.Context, brokers []string, csvPath string) error {
	// 1. Ensure the topic exists with 3 partitions.
	//if err := EnsureTopic(brokers, TopicSource, NumPartitions); err != nil {
	//	return err
	//}

	/*if err := RecreateTopic(brokers, TopicSource, NumPartitions); err != nil {
		return err
	}*/

	if topicExists(brokers, TopicSource) {
		if err := RecreateTopic(brokers, TopicSource, NumPartitions); err != nil {
			return err
		}
	} else {
		if err := EnsureTopic(brokers, TopicSource, NumPartitions); err != nil {
			return err
		}
	}
	// 2. Build the async producer.
	cfg := newAsyncProducerConfig()
	cfg.Producer.Partitioner = newRoundRobinPartitioner

	ap, err := sarama.NewAsyncProducer(brokers, cfg)
	if err != nil {
		return fmt.Errorf("new async producer: %w", err)
	}

	// 3. Drain success/error channels in a background goroutine.
	//    Without draining, the producer's internal buffer fills and deadlocks.
	var (
		sent   int64
		failed int64
	)
	var drainWg sync.WaitGroup
	drainWg.Add(1)
	go func() {
		defer drainWg.Done()
		for {
			select {
			case _, ok := <-ap.Successes():
				if !ok {
					return
				}
				n := atomic.AddInt64(&sent, 1)
				if n%progressInterval == 0 {
					fmt.Printf("  sent %d messages\n", n)
				}
			case e, ok := <-ap.Errors():
				if !ok {
					return
				}
				atomic.AddInt64(&failed, 1)
				// Log but don't abort — we retry via config
				fmt.Fprintf(os.Stderr, "produce error: %v\n", e.Err)
			}
		}
	}()

	// 4. Open the CSV file.
	f, err := os.Open(csvPath)
	if err != nil {
		return fmt.Errorf("open csv %q: %w", csvPath, err)
	}
	defer f.Close()

	// 5. Launch the worker pool.
	//    Each worker receives raw CSV lines, validates/trims them, and
	//    enqueues ProducerMessages onto the async producer's input channel.
	lines := make(chan string, channelBuffer)
	var workerWg sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			buf := make([]string, 0, batchSize)

			for line := range lines {
				buf = append(buf, line)
				if len(buf) < batchSize {
					continue
				}
				sendBatch(ap, buf)
				buf = buf[:0]
			}
			// Flush any remaining lines.
			if len(buf) > 0 {
				sendBatch(ap, buf)
			}
		}()
	}

	// 6. Reader goroutine — scans the CSV and feeds the lines channel.
	start := time.Now()
	scanner := bufio.NewScanner(f)

	// Increase the scanner buffer to handle long lines safely.
	const scanBuf = 1 * 1024 * 1024 // 1 MB
	scanner.Buffer(make([]byte, scanBuf), scanBuf)

	headerSkipped := false
	for scanner.Scan() {
		line := scanner.Text()
		if !headerSkipped {
			headerSkipped = true // skip the CSV header row
			continue
		}
		select {
		case lines <- line:
		case <-ctx.Done():
			break
		}
	}
	close(lines)

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("csv scan: %w", err)
	}

	// 7. Wait for all workers to finish, then close the producer.
	workerWg.Wait()
	ap.AsyncClose() // signals the broker we're done; flushes pending batches

	// 8. Wait for the drainer to see all acks/errors.
	drainWg.Wait()

	elapsed := time.Since(start)
	fmt.Printf("\nKafka producer finished.\n")
	fmt.Printf("  Messages sent:   %d\n", atomic.LoadInt64(&sent))
	fmt.Printf("  Messages failed: %d\n", atomic.LoadInt64(&failed))
	fmt.Printf("  Wall-clock time: %.2fs\n", elapsed.Seconds())
	fmt.Printf("  Throughput:      %.0f msg/s\n",
		float64(atomic.LoadInt64(&sent))/elapsed.Seconds())

	return nil
}

// sendBatch enqueues a slice of raw CSV lines onto the async producer.
// Each line becomes one Kafka message; the value is the raw CSV bytes.
// No key is set — the round-robin partitioner distributes by position.
func sendBatch(ap sarama.AsyncProducer, lines []string) {
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		// Quick sanity-check: a valid row has exactly 3 commas.
		if strings.Count(line, ",") < 3 {
			continue
		}
		ap.Input() <- &sarama.ProducerMessage{
			Topic: TopicSource,
			Value: sarama.StringEncoder(line),
		}
	}
}

// ─── CSV parsing helper (reused by consumer) ────────────────────────────────

// ParseCSVLine parses a raw "id,name,address,continent" line into a Person.
// Returns an error if the line has fewer than 4 fields.
func ParseCSVLine(line string) (*data.Person, error) {
	// Split on comma — the address field can contain spaces but no commas,
	// so a simple split is correct here.
	parts := strings.SplitN(line, ",", 4)
	if len(parts) != 4 {
		return nil, fmt.Errorf("expected 4 fields, got %d: %q", len(parts), line)
	}

	id64, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 32)
	if err != nil {
		return nil, fmt.Errorf("parse id %q: %w", parts[0], err)
	}

	return &data.Person{
		ID:        int32(id64),
		Name:      strings.TrimSpace(parts[1]),
		Address:   strings.TrimSpace(parts[2]),
		Continent: strings.TrimSpace(parts[3]),
	}, nil
}
