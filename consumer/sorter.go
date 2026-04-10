package consumer

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AnirudhV16/DataSorter/data"
	"github.com/AnirudhV16/DataSorter/producer"
	"github.com/IBM/sarama"
)

// ─── Constants ────────────────────────────────────────────────────────────────

const (
	progressInterval = 5_000_000
	outputPartitions = 1
)

// ─── Sort key ─────────────────────────────────────────────────────────────────

type SortKey string

const (
	SortByID        SortKey = "id"
	SortByName      SortKey = "name"
	SortByContinent SortKey = "continent"
)

func (k SortKey) OutputTopic() string { return string(k) }

// ─── Comparators ──────────────────────────────────────────────────────────────

type lessFunc func(a, b *data.Person) bool

func lessFor(key SortKey) (lessFunc, error) {
	switch key {
	case SortByID:
		return func(a, b *data.Person) bool { return a.ID < b.ID }, nil
	case SortByName:
		return func(a, b *data.Person) bool { return a.Name < b.Name }, nil
	case SortByContinent:
		return func(a, b *data.Person) bool { return a.Continent < b.Continent }, nil
	default:
		return nil, fmt.Errorf("unknown sort key %q", key)
	}
}

// ─── Sorter ───────────────────────────────────────────────────────────────────

// Sorter reads every message from the "source" topic, sorts by its key,
// then writes the sorted records to a single-partition output topic.
//
// Why a direct partition consumer instead of a consumer group?
//
//	Consumer groups are designed for ongoing infinite streams. For a bounded
//	"read everything once then stop" workload they have two problems:
//	  1. The group.Consume() loop re-enters after each session ends, so the
//	     handler receives every message again — producing the 150M symptom.
//	  2. There is no reliable way to detect "topic fully consumed" from inside
//	     a consumer group handler because the session can end at any time.
//
//	A direct partition consumer solves both:
//	  a. We query the exact high-water mark (HWM) per partition upfront.
//	     HWM = the offset of the *next* message to be written, so we know
//	     every message currently in the topic sits at offsets [0, HWM-1].
//	  b. Each goroutine stops the moment it reads the message at HWM-1.
//	     No polling, no re-entry, no infinite loops.
type Sorter struct {
	brokers []string
	key     SortKey
	less    lessFunc
}

// NewSorter constructs a Sorter for the given sort key.
func NewSorter(brokers []string, key SortKey) (*Sorter, error) {
	less, err := lessFor(key)
	if err != nil {
		return nil, err
	}
	return &Sorter{brokers: brokers, key: key, less: less}, nil
}

// Run executes the full consume → sort → produce pipeline.
func (s *Sorter) Run(ctx context.Context) error {
	start := time.Now()
	fmt.Printf("[%s] starting\n", s.key)

	// Ensure the output topic exists (1 partition preserves sort order).
	if err := producer.EnsureTopic(s.brokers, s.key.OutputTopic(), outputPartitions); err != nil {
		return fmt.Errorf("[%s] ensure topic: %w", s.key, err)
	}

	// Phase 1 — consume.
	records, err := s.consumeAll(ctx)
	if err != nil {
		return fmt.Errorf("[%s] consume: %w", s.key, err)
	}
	fmt.Printf("[%s] consumed  %d records in %.2fs\n",
		s.key, len(records), time.Since(start).Seconds())

	// Phase 2 — sort in-place using Go's pdqsort (O(n log n)).
	t1 := time.Now()
	sort.Slice(records, func(i, j int) bool { return s.less(records[i], records[j]) })
	fmt.Printf("[%s] sorted    %d records in %.2fs\n",
		s.key, len(records), time.Since(t1).Seconds())

	// Phase 3 — produce to output topic.
	t2 := time.Now()
	if err := s.produceAll(ctx, records); err != nil {
		return fmt.Errorf("[%s] produce: %w", s.key, err)
	}
	fmt.Printf("[%s] produced  %d records in %.2fs\n",
		s.key, len(records), time.Since(t2).Seconds())

	fmt.Printf("[%s] total wall-clock: %.2fs\n\n", s.key, time.Since(start).Seconds())
	return nil
}

// ─── Phase 1: consume ─────────────────────────────────────────────────────────

// consumeAll reads every message from every partition of the source topic,
// stopping precisely at each partition's high-water mark.
func (s *Sorter) consumeAll(ctx context.Context) ([]*data.Person, error) {
	client, err := sarama.NewClient(s.brokers, NewClientConfig())
	if err != nil {
		return nil, fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	// List all partitions for the source topic.
	partitions, err := client.Partitions(producer.TopicSource)
	if err != nil {
		return nil, fmt.Errorf("list partitions: %w", err)
	}

	// Query the high-water mark for every partition BEFORE reading.
	// HWM = next offset to be written = total messages in partition.
	// We will stop reading each partition at exactly offset HWM-1.
	// AFTER — query both oldest and HWM; count = HWM - oldest
	type partitionBounds struct {
		startOffset int64
		hwm         int64
	}
	bounds := make(map[int32]partitionBounds, len(partitions))
	totalMessages := int64(0)

	for _, p := range partitions {
		oldest, err := client.GetOffset(producer.TopicSource, p, sarama.OffsetOldest)
		if err != nil {
			return nil, fmt.Errorf("get oldest offset partition %d: %w", p, err)
		}
		hwm, err := client.GetOffset(producer.TopicSource, p, sarama.OffsetNewest)
		if err != nil {
			return nil, fmt.Errorf("get HWM partition %d: %w", p, err)
		}
		count := hwm - oldest
		bounds[p] = partitionBounds{startOffset: oldest, hwm: hwm}
		totalMessages += count
		fmt.Printf("[%s] partition %d  oldest=%d  HWM=%d  count=%d\n",
			s.key, p, oldest, hwm, count)
	}
	fmt.Printf("[%s] total messages to consume: %d\n", s.key, totalMessages)

	if totalMessages == 0 {
		return nil, fmt.Errorf("source topic is empty — run the producer first")
	}

	// Build the direct consumer.
	directConsumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("new consumer: %w", err)
	}
	defer directConsumer.Close()

	// Pre-allocate the result slice to the exact expected size to avoid
	// repeated growth copies during concurrent appends.
	records := make([]*data.Person, 0, totalMessages)
	var mu sync.Mutex
	var consumed int64 // shared atomic progress counter

	var wg sync.WaitGroup
	errCh := make(chan error, len(partitions))

	// AFTER — uses bounds struct; stop condition uses pb.hwm which is correct
	for _, partID := range partitions {
		b := bounds[partID]
		if b.hwm == b.startOffset {
			fmt.Printf("[%s] partition %d is empty, skipping\n", s.key, partID)
			continue
		}

		wg.Add(1)
		go func(pid int32, pb partitionBounds) {
			defer wg.Done()

			pc, err := directConsumer.ConsumePartition(
				producer.TopicSource, pid, sarama.OffsetOldest)
			if err != nil {
				errCh <- fmt.Errorf("consume partition %d: %w", pid, err)
				return
			}
			defer pc.Close()

			for {
				select {
				case <-ctx.Done():
					return
				case msg, ok := <-pc.Messages():
					if !ok {
						return
					}
					p, err := producer.ParseCSVLine(string(msg.Value))
					if err != nil {
						fmt.Fprintf(os.Stderr,
							"[%s] parse error partition=%d offset=%d: %v\n",
							s.key, pid, msg.Offset, err)
					} else {
						mu.Lock()
						records = append(records, p)
						mu.Unlock()
					}
					n := atomic.AddInt64(&consumed, 1)
					if n%progressInterval == 0 {
						fmt.Printf("  [%s] consumed %d / %d records\n",
							s.key, n, totalMessages)
					}
					// Stop at the last message that existed when we queried.
					if msg.Offset >= pb.hwm-1 {
						return
					}
				}
			}
		}(partID, b)
	}

	wg.Wait()
	close(errCh)

	for e := range errCh {
		if e != nil {
			return nil, e
		}
	}

	return records, nil
}

// newClientConfig returns a Sarama config tuned for bulk partition reads.
func NewClientConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_6_0_0

	cfg.Consumer.Fetch.Min = 1
	cfg.Consumer.Fetch.Default = 10 * 1024 * 1024 // 10 MB per fetch request
	cfg.Consumer.Fetch.Max = 50 * 1024 * 1024     // 50 MB hard cap per fetch

	// Wait up to 500 ms for a full batch before returning partial data.
	cfg.Consumer.MaxWaitTime = 500 * time.Millisecond
	cfg.Consumer.Retry.Backoff = 200 * time.Millisecond

	return cfg
}

// ─── Phase 3: produce ─────────────────────────────────────────────────────────

// produceAll streams the sorted slice to the single-partition output topic.
// Single partition guarantees the downstream consumer receives records in the
// exact order they were written — i.e., fully sorted.
func (s *Sorter) produceAll(ctx context.Context, records []*data.Person) error {
	ap, err := sarama.NewAsyncProducer(s.brokers, newOutputProducerConfig())
	if err != nil {
		return fmt.Errorf("async producer: %w", err)
	}

	outTopic := s.key.OutputTopic()

	// Drain ack/error channels in background to prevent internal deadlock.
	var sent, failed int64
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
					fmt.Printf("  [%s] produced %d records\n", s.key, n)
				}
			case e, ok := <-ap.Errors():
				if !ok {
					return
				}
				atomic.AddInt64(&failed, 1)
				fmt.Fprintf(os.Stderr, "[%s] produce error: %v\n", s.key, e.Err)
			}
		}
	}()

	for _, p := range records {
		select {
		case <-ctx.Done():
			goto done
		case ap.Input() <- &sarama.ProducerMessage{
			Topic: outTopic,
			Value: sarama.StringEncoder(p.ToCSV()),
			// No key needed — single partition preserves insertion order.
		}:
		}
	}

done:
	ap.AsyncClose() // flush remaining batches, then signal the broker we are done
	drainWg.Wait()

	fmt.Printf("[%s] output topic=%q  sent=%d  failed=%d\n",
		s.key, outTopic, atomic.LoadInt64(&sent), atomic.LoadInt64(&failed))
	return nil
}

// newOutputProducerConfig is tuned for high-throughput sequential writes.
func newOutputProducerConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_6_0_0

	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Compression = sarama.CompressionSnappy
	cfg.Producer.Flush.Messages = 64_000
	cfg.Producer.Flush.Frequency = 100 * time.Millisecond
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Retry.Backoff = 200 * time.Millisecond
	cfg.ChannelBufferSize = 1_000_000
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true

	return cfg
}
