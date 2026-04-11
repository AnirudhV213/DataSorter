package consumer

import (
	"bufio"
	"container/heap"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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

	// chunkSize is how many records we hold in RAM at once during Phase 1.
	// 500K records × ~130 bytes ≈ 65 MB — comfortably within the 2 GB budget
	// even with three sorters sharing the process sequentially.
	chunkSize = 500_000
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

// Sorter implements an external merge sort so the full 50M dataset is never
// held in RAM simultaneously.
//
// Algorithm — two phases:
//
//  1. CHUNK SORT
//     Read the source Kafka topic in chunks of `chunkSize` records.
//     Sort each chunk in memory (≈65 MB peak per chunk).
//     Write the sorted chunk to a temp CSV file on disk.
//     Discard the chunk from RAM before reading the next one.
//
//  2. K-WAY MERGE
//     Open all temp files simultaneously.
//     Use a min-heap (one entry per file) to merge them in sort order.
//     Only one record per temp file is live in RAM at any moment.
//     Stream the merged output directly to the Kafka output topic.
//     Delete temp files when done.
//
// Memory profile:
//
//	Phase 1: chunkSize × ~130 B ≈ 65 MB
//	Phase 2: numChunks × ~130 B (one record per file) + heap overhead ≈ ~13 MB
//	          for 100 chunks of 500K records each
//	Total peak: ~65 MB — fits easily within the 2 GB budget.
type Sorter struct {
	brokers []string
	key     SortKey
	less    lessFunc
	tmpDir  string // directory for temp chunk files
}

// NewSorter constructs a Sorter for the given sort key.
func NewSorter(brokers []string, key SortKey) (*Sorter, error) {
	less, err := lessFor(key)
	if err != nil {
		return nil, err
	}
	// Create a dedicated temp directory for this sorter's chunk files.
	tmpDir, err := os.MkdirTemp("", "sorter-"+string(key)+"-*")
	if err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}
	return &Sorter{brokers: brokers, key: key, less: less, tmpDir: tmpDir}, nil
}

// Run executes the full pipeline: consume → chunk-sort → k-way merge → produce.
func (s *Sorter) Run(ctx context.Context) error {
	start := time.Now()
	fmt.Printf("[%s] starting (external merge sort)\n", s.key)

	// Clean up temp files when done regardless of outcome.
	defer func() {
		os.RemoveAll(s.tmpDir)
		fmt.Printf("[%s] temp dir cleaned up\n", s.key)
	}()

	// Ensure output topic (1 partition preserves merge order).
	if err := producer.EnsureTopic(s.brokers, s.key.OutputTopic(), outputPartitions); err != nil {
		return fmt.Errorf("[%s] ensure topic: %w", s.key, err)
	}

	// Phase 1 — consume from Kafka, sort in chunks, write to temp files.
	t1 := time.Now()
	chunkFiles, totalRecords, err := s.chunkSort(ctx)
	if err != nil {
		return fmt.Errorf("[%s] chunk sort: %w", s.key, err)
	}
	fmt.Printf("[%s] phase1 chunk-sort: %d records → %d chunk files in %.2fs\n",
		s.key, totalRecords, len(chunkFiles), time.Since(t1).Seconds())

	// Phase 2 — k-way merge chunk files, stream to Kafka output topic.
	t2 := time.Now()
	if err := s.mergeAndProduce(ctx, chunkFiles); err != nil {
		return fmt.Errorf("[%s] merge+produce: %w", s.key, err)
	}
	fmt.Printf("[%s] phase2 merge+produce: done in %.2fs\n",
		s.key, time.Since(t2).Seconds())

	fmt.Printf("[%s] total wall-clock: %.2fs\n\n", s.key, time.Since(start).Seconds())
	return nil
}

// ─── Phase 1: chunk sort ──────────────────────────────────────────────────────

// chunkSort reads the source topic in chunks of `chunkSize`, sorts each chunk
// in memory, and writes it to a temp CSV file.
// Returns the list of temp file paths and total record count.
func (s *Sorter) chunkSort(ctx context.Context) ([]string, int64, error) {
	client, err := sarama.NewClient(s.brokers, NewClientConfig())
	if err != nil {
		return nil, 0, fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	partitions, err := client.Partitions(producer.TopicSource)
	if err != nil {
		return nil, 0, fmt.Errorf("list partitions: %w", err)
	}

	// Collect per-partition stop offsets.
	type pBounds struct{ oldest, hwm int64 }
	bounds := make(map[int32]pBounds)
	for _, p := range partitions {
		oldest, err := client.GetOffset(producer.TopicSource, p, sarama.OffsetOldest)
		if err != nil {
			return nil, 0, fmt.Errorf("oldest p%d: %w", p, err)
		}
		hwm, err := client.GetOffset(producer.TopicSource, p, sarama.OffsetNewest)
		if err != nil {
			return nil, 0, fmt.Errorf("hwm p%d: %w", p, err)
		}
		bounds[p] = pBounds{oldest, hwm}
		fmt.Printf("[%s] partition %d  oldest=%d  HWM=%d  count=%d\n",
			s.key, p, oldest, hwm, hwm-oldest)
	}

	directConsumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, 0, fmt.Errorf("new consumer: %w", err)
	}
	defer directConsumer.Close()

	// msgCh fans in messages from all partition goroutines into one channel.
	// Buffer = chunkSize so partition readers are never blocked by the
	// chunk writer.
	msgCh := make(chan *data.Person, chunkSize/10)

	// Fan-in: one goroutine per partition feeds msgCh.
	fanInDone := make(chan struct{})
	var activePartitions int64 = int64(len(partitions))
	for _, partID := range partitions {
		b := bounds[partID]
		if b.hwm == b.oldest {
			atomic.AddInt64(&activePartitions, -1)
			continue
		}
		go func(pid int32, pb pBounds) {
			pc, err := directConsumer.ConsumePartition(
				producer.TopicSource, pid, sarama.OffsetOldest)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[%s] consume p%d: %v\n", s.key, pid, err)
				if atomic.AddInt64(&activePartitions, -1) == 0 {
					close(fanInDone)
				}
				return
			}
			defer pc.Close()
			for {
				select {
				case <-ctx.Done():
					if atomic.AddInt64(&activePartitions, -1) == 0 {
						close(fanInDone)
					}
					return
				case msg, ok := <-pc.Messages():
					if !ok {
						if atomic.AddInt64(&activePartitions, -1) == 0 {
							close(fanInDone)
						}
						return
					}
					p, err := producer.ParseCSVLine(string(msg.Value))
					if err == nil {
						msgCh <- p
					}
					if msg.Offset >= pb.hwm-1 {
						if atomic.AddInt64(&activePartitions, -1) == 0 {
							close(fanInDone)
						}
						return
					}
				}
			}
		}(partID, b)
	}

	// Close msgCh once all partition goroutines finish.
	go func() {
		<-fanInDone
		close(msgCh)
	}()

	// Chunk writer: drain msgCh, fill a chunk, sort it, flush to disk.
	var chunkFiles []string
	var totalRecords int64
	chunk := make([]*data.Person, 0, chunkSize)
	consumed := int64(0)

	flushChunk := func() error {
		if len(chunk) == 0 {
			return nil
		}
		sort.Slice(chunk, func(i, j int) bool { return s.less(chunk[i], chunk[j]) })
		path, err := s.writeChunkFile(chunk, len(chunkFiles))
		if err != nil {
			return err
		}
		chunkFiles = append(chunkFiles, path)
		totalRecords += int64(len(chunk))
		chunk = chunk[:0] // reuse backing array — no new allocation
		return nil
	}

	for p := range msgCh {
		chunk = append(chunk, p)
		consumed++
		if consumed%progressInterval == 0 {
			fmt.Printf("  [%s] consumed %d records, %d chunks written\n",
				s.key, consumed, len(chunkFiles))
		}
		if len(chunk) >= chunkSize {
			if err := flushChunk(); err != nil {
				return nil, 0, err
			}
		}
	}
	// Flush the final partial chunk.
	if err := flushChunk(); err != nil {
		return nil, 0, err
	}

	return chunkFiles, totalRecords, nil
}

// writeChunkFile writes a sorted chunk to a temp CSV file and returns the path.
func (s *Sorter) writeChunkFile(chunk []*data.Person, idx int) (string, error) {
	path := filepath.Join(s.tmpDir, fmt.Sprintf("chunk_%05d.csv", idx))
	f, err := os.Create(path)
	if err != nil {
		return "", fmt.Errorf("create chunk file: %w", err)
	}
	defer f.Close()

	w := bufio.NewWriterSize(f, 4*1024*1024) // 4 MB write buffer
	for _, p := range chunk {
		if _, err := fmt.Fprintln(w, p.ToCSV()); err != nil {
			return "", fmt.Errorf("write chunk record: %w", err)
		}
	}
	return path, w.Flush()
}

// ─── Phase 2: k-way merge ────────────────────────────────────────────────────

// mergeAndProduce opens all chunk files, merges them in sort order using a
// min-heap, and streams each record directly to the Kafka output topic.
func (s *Sorter) mergeAndProduce(ctx context.Context, chunkFiles []string) error {
	// Open all chunk files and seed the heap with the first record from each.
	h := &mergeHeap{less: s.less}
	readers := make([]*chunkReader, 0, len(chunkFiles))

	for i, path := range chunkFiles {
		r, err := newChunkReader(path, i)
		if err != nil {
			return fmt.Errorf("open chunk %d: %w", i, err)
		}
		readers = append(readers, r)
		if p := r.peek(); p != nil {
			heap.Push(h, &heapEntry{person: p, readerIdx: i})
			r.advance()
		}
	}
	defer func() {
		for _, r := range readers {
			r.close()
		}
	}()

	heap.Init(h)

	// Build the Kafka async producer for the output topic.
	ap, err := sarama.NewAsyncProducer(s.brokers, newOutputProducerConfig())
	if err != nil {
		return fmt.Errorf("async producer: %w", err)
	}

	outTopic := s.key.OutputTopic()
	var sent, failed int64

	// Drain ack/error channels in background.
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
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

	// Pop smallest record from heap, send to Kafka, push next from same file.
	for h.Len() > 0 {
		select {
		case <-ctx.Done():
			goto done
		default:
		}

		entry := heap.Pop(h).(*heapEntry)
		ap.Input() <- &sarama.ProducerMessage{
			Topic: outTopic,
			Value: sarama.StringEncoder(entry.person.ToCSV()),
		}

		// Advance the reader this entry came from and push its next record.
		r := readers[entry.readerIdx]
		if next := r.peek(); next != nil {
			heap.Push(h, &heapEntry{person: next, readerIdx: entry.readerIdx})
			r.advance()
		}
	}

done:
	ap.AsyncClose()
	<-drainDone

	fmt.Printf("[%s] output topic=%q  sent=%d  failed=%d\n",
		s.key, outTopic, atomic.LoadInt64(&sent), atomic.LoadInt64(&failed))
	return nil
}

// ─── Chunk file reader ────────────────────────────────────────────────────────

// chunkReader reads Person records one at a time from a sorted temp CSV file.
type chunkReader struct {
	idx    int
	file   *os.File
	reader *csv.Reader
	next   *data.Person // pre-read next record (nil = EOF)
}

func newChunkReader(path string, idx int) (*chunkReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r := &chunkReader{idx: idx, file: f, reader: csv.NewReader(bufio.NewReaderSize(f, 32*1024))}
	r.reader.FieldsPerRecord = 4
	r.reader.ReuseRecord = true // reuse backing array — avoids per-row allocation
	r.advance()                 // pre-load first record
	return r, nil
}

// peek returns the next record without consuming it (nil at EOF).
func (r *chunkReader) peek() *data.Person { return r.next }

// advance reads the next record from the file into r.next.
func (r *chunkReader) advance() {
	fields, err := r.reader.Read()
	if err == io.EOF {
		r.next = nil
		return
	}
	if err != nil {
		r.next = nil
		return
	}
	id64, err := strconv.ParseInt(strings.TrimSpace(fields[0]), 10, 32)
	if err != nil {
		r.next = nil
		return
	}
	// Copy strings out of the reused record buffer before the next Read() call
	// overwrites them.
	r.next = &data.Person{
		ID:        int32(id64),
		Name:      strings.TrimSpace(fields[1]),
		Address:   strings.TrimSpace(fields[2]),
		Continent: strings.TrimSpace(fields[3]),
	}
}

func (r *chunkReader) close() {
	r.file.Close()
}

// ─── Min-heap for k-way merge ─────────────────────────────────────────────────

type heapEntry struct {
	person    *data.Person
	readerIdx int
}

type mergeHeap struct {
	entries []*heapEntry
	less    lessFunc
}

func (h *mergeHeap) Len() int { return len(h.entries) }
func (h *mergeHeap) Less(i, j int) bool {
	return h.less(h.entries[i].person, h.entries[j].person)
}
func (h *mergeHeap) Swap(i, j int) { h.entries[i], h.entries[j] = h.entries[j], h.entries[i] }
func (h *mergeHeap) Push(x any)    { h.entries = append(h.entries, x.(*heapEntry)) }
func (h *mergeHeap) Pop() any {
	n := len(h.entries)
	x := h.entries[n-1]
	h.entries = h.entries[:n-1]
	return x
}

// ─── Shared configs ───────────────────────────────────────────────────────────

// NewClientConfig is exported so verify.go can reuse it.
func NewClientConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_6_0_0
	cfg.Consumer.Fetch.Min = 1
	cfg.Consumer.Fetch.Default = 10 * 1024 * 1024
	cfg.Consumer.Fetch.Max = 50 * 1024 * 1024
	cfg.Consumer.MaxWaitTime = 500 * time.Millisecond
	cfg.Consumer.Retry.Backoff = 200 * time.Millisecond
	return cfg
}

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
