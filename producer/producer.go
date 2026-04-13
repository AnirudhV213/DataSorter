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
	TopicSource = "source"

	NumPartitions = 3

	// workerCount: number of goroutines feeding the async producer.
	workerCount = 4

	// batchSize: CSV lines accumulated per worker before sending to producer.
	batchSize = 500

	// channelBuffer: depth of the line-distribution channel.
	channelBuffer = 5000

	progressInterval = 5_000_000
)

// ─── Topic admin ────────────────────────────────────────────────────────────

// EnsureTopic creates the topic if it does not already exist (idempotent).
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
		ReplicationFactor: 1,
	}

	err = admin.CreateTopic(topic, detail, false)
	if err != nil {
		if kafkaErr, ok := err.(*sarama.TopicError); ok &&
			kafkaErr.Err == sarama.ErrTopicAlreadyExists {
			return nil
		}
		return fmt.Errorf("create topic %q: %w", topic, err)
	}

	fmt.Printf("Topic %q created with %d partitions\n", topic, partitions)
	return nil
}

func newAdmin(brokers []string) (sarama.ClusterAdmin, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_6_0_0
	admin, err := sarama.NewClusterAdmin(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("cluster admin: %w", err)
	}
	return admin, nil
}

// RecreateTopic deletes the topic if it exists, then creates it fresh,
// guaranteeing HWM=0 at the start of each run.
func RecreateTopic(brokers []string, topic string, partitions int32) error {
	admin, err := newAdmin(brokers)
	if err != nil {
		return err
	}
	defer admin.Close()

	if err := admin.DeleteTopic(topic); err != nil {
		kafkaErr, ok := err.(*sarama.TopicError)
		if !ok || kafkaErr.Err != sarama.ErrUnknownTopicOrPartition {
			return fmt.Errorf("delete topic %q: %w", topic, err)
		}
	} else {
		fmt.Printf("Topic %q deleted (clearing stale data)\n", topic)
		// Broker deletion is async — wait before recreating to avoid a
		// spurious ErrTopicAlreadyExists immediately after delete.
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

// newAsyncProducerConfig returns a Sarama config tuned for high throughput.
// Snappy compression, large batch linger, and WaitForAll acks balance
// durability against produce latency on a single-broker setup.
func newAsyncProducerConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_6_0_0
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Compression = sarama.CompressionSnappy
	cfg.Producer.Flush.Messages = 5000
	cfg.Producer.Flush.Frequency = 100 * time.Millisecond
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Retry.Backoff = 200 * time.Millisecond
	cfg.ChannelBufferSize = 10_000
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	return cfg
}

// ─── Partition router ───────────────────────────────────────────────────────

// roundRobinPartitioner distributes messages uniformly across partitions.
type roundRobinPartitioner struct {
	counter uint64
}

func (r *roundRobinPartitioner) Partition(msg *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	idx := atomic.AddUint64(&r.counter, 1) - 1
	return int32(idx % uint64(numPartitions)), nil
}

func (r *roundRobinPartitioner) RequiresConsistency() bool { return false }

func newRoundRobinPartitioner(_ string) sarama.Partitioner {
	return &roundRobinPartitioner{}
}

// ─── CSV → Kafka pipeline ───────────────────────────────────────────────────

// SendCSVToKafka reads csvPath line-by-line and sends every data row to the
// Kafka source topic using a reader goroutine + worker pool pattern:
//
//	┌────────┐  lines chan  ┌──────────────────────┐  ProducerMessage
//	│ reader │─────────────▶│  workerCount workers  │──────────────────▶ Kafka
//	└────────┘              └──────────────────────┘
func SendCSVToKafka(ctx context.Context, brokers []string, csvPath string) error {
	if topicExists(brokers, TopicSource) {
		if err := RecreateTopic(brokers, TopicSource, NumPartitions); err != nil {
			return err
		}
	} else {
		if err := EnsureTopic(brokers, TopicSource, NumPartitions); err != nil {
			return err
		}
	}

	cfg := newAsyncProducerConfig()
	cfg.Producer.Partitioner = newRoundRobinPartitioner

	ap, err := sarama.NewAsyncProducer(brokers, cfg)
	if err != nil {
		return fmt.Errorf("new async producer: %w", err)
	}

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
				fmt.Fprintf(os.Stderr, "produce error: %v\n", e.Err)
			}
		}
	}()

	f, err := os.Open(csvPath)
	if err != nil {
		return fmt.Errorf("open csv %q: %w", csvPath, err)
	}
	defer f.Close()

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
			if len(buf) > 0 {
				sendBatch(ap, buf)
			}
		}()
	}

	start := time.Now()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1*1024*1024), 1*1024*1024)

	headerSkipped := false
	for scanner.Scan() {
		line := scanner.Text()
		if !headerSkipped {
			headerSkipped = true
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

	workerWg.Wait()
	ap.AsyncClose()
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

func sendBatch(ap sarama.AsyncProducer, lines []string) {
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.Count(line, ",") < 3 {
			continue
		}
		ap.Input() <- &sarama.ProducerMessage{
			Topic: TopicSource,
			Value: sarama.StringEncoder(line),
		}
	}
}

// ─── Helpers ────────────────────────────────────────────────────────────────

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

// ParseCSVLine parses a raw "id,name,address,continent" line into a Person.
func ParseCSVLine(line string) (*data.Person, error) {
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
