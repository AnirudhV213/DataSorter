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

	// chunkSize: 500K records × ~130 bytes = ~65 MB per sorter.
	// With 3 parallel sorters: 3 × 65 MB = 195 MB peak for chunks.
	chunkSize = 500_000

	// lowCardinalityThreshold: if the number of distinct values seen during
	// the first chunkSize records is below this, switch to bucket sort.
	// continent has exactly 6 values — well below this threshold.
	// id and name have cardinality ≈ 50M — well above it.
	lowCardinalityThreshold = 200
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

// keyOf extracts the sort key string from a Person for cardinality probing.
func keyOf(key SortKey, p *data.Person) string {
	switch key {
	case SortByID:
		return fmt.Sprintf("%d", p.ID)
	case SortByName:
		return p.Name
	case SortByContinent:
		return p.Continent
	}
	return ""
}

// ─── Sorter ───────────────────────────────────────────────────────────────────

// Sorter implements an external merge sort (high-cardinality keys like id/name)
// or a bucket sort (low-cardinality keys like continent) — chosen automatically
// by probing the first chunk for distinct value count.
//
// Bucket sort algorithm (continent):
//
//  1. BUCKET FILL
//     Consume all 50M records from Kafka.
//     Route each record into one of N bucket files (one per distinct value)
//     using a map[string]*bufio.Writer. No sorting at all in this phase —
//     it's a pure O(N) partitioning pass.
//
//  2. SORT EACH BUCKET + STREAM
//     For each bucket file (sorted key order):
//     - Read the entire bucket into memory (≈50M/6 ≈ 8.3M records × 130B ≈ 1.08 GB)
//
//     Wait — that's too large. Instead we do a mini external-merge within each
//     bucket: read in chunkSize sub-chunks, sort each, write sub-chunk files,
//     then k-way merge the sub-chunks for that bucket and stream to Kafka.
//     Because all records in a bucket share the same key value, the merge is
//     trivially correct (any order within the bucket is valid for a stable sort
//     on continent alone).
//
//     Actually for continent sort, records within the same continent can be in
//     ANY order — the spec only says "sorted by continent". So we can skip the
//     within-bucket sort entirely and just stream each bucket sequentially.
//
// Memory profile (bucket sort):
//
//	Bucket writers: 6 open file handles + 256 KB buffers each ≈ 1.5 MB
//	msgCh buffer:  50K × 130 B ≈ 6.5 MB
//	No chunk array needed in Phase 1 — records go straight to bucket files.
//	Phase 2: one bucket read at a time via streaming — O(1) memory.
//
// External merge sort algorithm (id / name) — unchanged from before:
//
//  1. Read source topic in chunks of chunkSize, sort each in memory, write to temp file.
//  2. K-way heap merge all temp files → stream to Kafka output topic.
type Sorter struct {
	brokers []string
	key     SortKey
	less    lessFunc
	tmpDir  string
}

func NewSorter(brokers []string, key SortKey) (*Sorter, error) {
	less, err := lessFor(key)
	if err != nil {
		return nil, err
	}
	tmpDir, err := os.MkdirTemp("", "sorter-"+string(key)+"-*")
	if err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}
	return &Sorter{brokers: brokers, key: key, less: less, tmpDir: tmpDir}, nil
}

// Run dispatches to bucket sort or external merge sort based on cardinality.
func (s *Sorter) Run(ctx context.Context) error {
	start := time.Now()
	fmt.Printf("[%s] starting\n", s.key)

	defer func() {
		os.RemoveAll(s.tmpDir)
		fmt.Printf("[%s] temp dir cleaned up\n", s.key)
	}()

	if err := producer.EnsureTopic(s.brokers, s.key.OutputTopic(), outputPartitions); err != nil {
		return fmt.Errorf("[%s] ensure topic: %w", s.key, err)
	}

	// Probe cardinality using the first chunk's records.
	// If distinctValues < lowCardinalityThreshold → bucket sort.
	// Otherwise → external merge sort.
	//
	// We open the Kafka consumer once here for probing, then pass the
	// already-open consumer into whichever sort path we choose, so we
	// don't pay the connection cost twice.
	client, err := sarama.NewClient(s.brokers, newParallelClientConfig())
	if err != nil {
		return fmt.Errorf("[%s] new client: %w", s.key, err)
	}
	defer client.Close()

	partitions, err := client.Partitions(producer.TopicSource)
	if err != nil {
		return fmt.Errorf("[%s] list partitions: %w", s.key, err)
	}

	type pBounds struct{ oldest, hwm int64 }
	bounds := make(map[int32]pBounds)
	for _, p := range partitions {
		oldest, _ := client.GetOffset(producer.TopicSource, p, sarama.OffsetOldest)
		hwm, _ := client.GetOffset(producer.TopicSource, p, sarama.OffsetNewest)
		bounds[p] = pBounds{oldest, hwm}
		fmt.Printf("[%s] partition %d  oldest=%d  HWM=%d  count=%d\n",
			s.key, p, oldest, hwm, hwm-oldest)
	}

	directConsumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return fmt.Errorf("[%s] new consumer: %w", s.key, err)
	}
	defer directConsumer.Close()

	msgCh := make(chan *data.Person, 50_000)
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
					if p, err := producer.ParseCSVLine(string(msg.Value)); err == nil {
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

	go func() {
		<-fanInDone
		close(msgCh)
	}()

	// Drain the first chunkSize records to probe cardinality.
	probe := make([]*data.Person, 0, chunkSize)
	distinct := make(map[string]struct{}, lowCardinalityThreshold+1)
	for p := range msgCh {
		probe = append(probe, p)
		distinct[keyOf(s.key, p)] = struct{}{}
		if len(probe) >= chunkSize {
			break
		}
	}

	isLowCardinality := len(distinct) < lowCardinalityThreshold
	fmt.Printf("[%s] cardinality probe: %d distinct values in first %d records → using %s\n",
		s.key, len(distinct), len(probe), map[bool]string{true: "bucket sort", false: "external merge sort"}[isLowCardinality])

	var sortErr error
	if isLowCardinality {
		sortErr = s.bucketSort(ctx, probe, msgCh)
	} else {
		sortErr = s.externalMergeSort(ctx, probe, msgCh)
	}
	if sortErr != nil {
		return sortErr
	}

	fmt.Printf("[%s] total wall-clock: %.2fs\n\n", s.key, time.Since(start).Seconds())
	return nil
}

// ─── Bucket sort (low-cardinality: continent) ─────────────────────────────────

// bucketSort handles fields with few distinct values (e.g. continent = 6 values).
//
// Phase 1 — O(N) partitioning:
//   - One output file per distinct value, opened upfront.
//   - Each incoming record is routed to its bucket file by map lookup.
//   - No sorting, no chunk arrays — memory is O(numBuckets) ≈ negligible.
//
// Phase 2 — sequential streaming:
//   - Buckets are processed in sorted key order (sort.Strings on bucket names).
//   - Each bucket file is streamed line-by-line directly to Kafka.
//   - Records within a bucket need no further sorting because the spec only
//     requires ordering by the sort key (continent), not a secondary key.
//   - Memory usage during Phase 2: one read buffer per open file ≈ O(1).
//
// Total complexity: O(N) vs O(N log N) for external merge sort.
// For continent with N=50M this saves ~log₂(50M) ≈ 25 comparisons per record.
func (s *Sorter) bucketSort(ctx context.Context, probe []*data.Person, msgCh <-chan *data.Person) error {
	t1 := time.Now()
	fmt.Printf("[%s] phase1 bucket-fill starting\n", s.key)

	// Open one file per distinct bucket value.
	// We discover buckets lazily as records arrive.
	type bucketFile struct {
		file   *os.File
		writer *bufio.Writer
	}
	buckets := make(map[string]*bucketFile)

	getBucket := func(val string) (*bucketFile, error) {
		if b, ok := buckets[val]; ok {
			return b, nil
		}
		// Sanitise value for use as a filename (replace spaces with underscores).
		safe := strings.ReplaceAll(val, " ", "_")
		path := filepath.Join(s.tmpDir, fmt.Sprintf("bucket_%s.csv", safe))
		f, err := os.Create(path)
		if err != nil {
			return nil, fmt.Errorf("create bucket %q: %w", val, err)
		}
		bf := &bucketFile{
			file:   f,
			writer: bufio.NewWriterSize(f, 4*1024*1024), // 4 MB write buffer
		}
		buckets[val] = bf
		return bf, nil
	}

	closeAll := func() {
		for _, bf := range buckets {
			bf.writer.Flush()
			bf.file.Close()
		}
	}

	writeRecord := func(p *data.Person) error {
		val := keyOf(s.key, p)
		bf, err := getBucket(val)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(bf.writer, p.ToCSV())
		return err
	}

	// Write probe records (already consumed from msgCh during cardinality probe).
	consumed := int64(0)
	for _, p := range probe {
		if err := writeRecord(p); err != nil {
			closeAll()
			return fmt.Errorf("[%s] bucket write: %w", s.key, err)
		}
		consumed++
	}

	// Continue draining msgCh for the rest of the records.
	for p := range msgCh {
		select {
		case <-ctx.Done():
			closeAll()
			return ctx.Err()
		default:
		}
		if err := writeRecord(p); err != nil {
			closeAll()
			return fmt.Errorf("[%s] bucket write: %w", s.key, err)
		}
		consumed++
		if consumed%progressInterval == 0 {
			fmt.Printf("  [%s] bucketed %d records into %d buckets\n",
				s.key, consumed, len(buckets))
		}
	}

	closeAll()
	fmt.Printf("[%s] phase1 bucket-fill: %d records → %d buckets in %.2fs\n",
		s.key, consumed, len(buckets), time.Since(t1).Seconds())

	// Phase 2: stream buckets to Kafka in sorted key order.
	t2 := time.Now()
	fmt.Printf("[%s] phase2 bucket-stream starting\n", s.key)

	// Collect and sort bucket keys so output is in correct order.
	bucketKeys := make([]string, 0, len(buckets))
	for k := range buckets {
		bucketKeys = append(bucketKeys, k)
	}
	sort.Strings(bucketKeys)

	ap, err := sarama.NewAsyncProducer(s.brokers, newOutputProducerConfig())
	if err != nil {
		return fmt.Errorf("[%s] async producer: %w", s.key, err)
	}

	outTopic := s.key.OutputTopic()
	var sent, failed int64

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

	for _, bk := range bucketKeys {
		safe := strings.ReplaceAll(bk, " ", "_")
		path := filepath.Join(s.tmpDir, fmt.Sprintf("bucket_%s.csv", safe))

		f, err := os.Open(path)
		if err != nil {
			ap.AsyncClose()
			<-drainDone
			return fmt.Errorf("[%s] open bucket %q: %w", s.key, bk, err)
		}

		scanner := bufio.NewScanner(bufio.NewReaderSize(f, 4*1024*1024))
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				f.Close()
				ap.AsyncClose()
				<-drainDone
				return ctx.Err()
			default:
			}
			ap.Input() <- &sarama.ProducerMessage{
				Topic: outTopic,
				Value: sarama.StringEncoder(scanner.Text()),
			}
		}
		f.Close()
		if err := scanner.Err(); err != nil {
			ap.AsyncClose()
			<-drainDone
			return fmt.Errorf("[%s] scan bucket %q: %w", s.key, bk, err)
		}
	}

	ap.AsyncClose()
	<-drainDone

	fmt.Printf("[%s] phase2 bucket-stream: done in %.2fs\n",
		s.key, time.Since(t2).Seconds())
	fmt.Printf("[%s] output topic=%q  sent=%d  failed=%d\n",
		s.key, outTopic, atomic.LoadInt64(&sent), atomic.LoadInt64(&failed))
	return nil
}

// ─── External merge sort (high-cardinality: id / name) ───────────────────────

func (s *Sorter) externalMergeSort(ctx context.Context, probe []*data.Person, msgCh <-chan *data.Person) error {
	t1 := time.Now()

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
		chunk = chunk[:0]
		return nil
	}

	// Process probe records first (already consumed during cardinality check).
	for _, p := range probe {
		chunk = append(chunk, p)
		consumed++
		if len(chunk) >= chunkSize {
			if err := flushChunk(); err != nil {
				return err
			}
		}
	}

	// Continue with remaining records from msgCh.
	for p := range msgCh {
		chunk = append(chunk, p)
		consumed++
		if consumed%progressInterval == 0 {
			fmt.Printf("  [%s] consumed %d records, %d chunks written\n",
				s.key, consumed, len(chunkFiles))
		}
		if len(chunk) >= chunkSize {
			if err := flushChunk(); err != nil {
				return err
			}
		}
	}
	if err := flushChunk(); err != nil {
		return err
	}

	fmt.Printf("[%s] phase1 chunk-sort: %d records → %d chunk files in %.2fs\n",
		s.key, totalRecords, len(chunkFiles), time.Since(t1).Seconds())

	t2 := time.Now()
	if err := s.mergeAndProduce(ctx, chunkFiles); err != nil {
		return fmt.Errorf("[%s] merge+produce: %w", s.key, err)
	}
	fmt.Printf("[%s] phase2 merge+produce: done in %.2fs\n",
		s.key, time.Since(t2).Seconds())
	return nil
}

func (s *Sorter) writeChunkFile(chunk []*data.Person, idx int) (string, error) {
	path := filepath.Join(s.tmpDir, fmt.Sprintf("chunk_%05d.csv", idx))
	f, err := os.Create(path)
	if err != nil {
		return "", fmt.Errorf("create chunk file: %w", err)
	}
	defer f.Close()

	w := bufio.NewWriterSize(f, 4*1024*1024)
	for _, p := range chunk {
		if _, err := fmt.Fprintln(w, p.ToCSV()); err != nil {
			return "", fmt.Errorf("write chunk record: %w", err)
		}
	}
	return path, w.Flush()
}

// ─── Phase 2: k-way merge (used by external merge sort path only) ─────────────

func (s *Sorter) mergeAndProduce(ctx context.Context, chunkFiles []string) error {
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

	ap, err := sarama.NewAsyncProducer(s.brokers, newOutputProducerConfig())
	if err != nil {
		return fmt.Errorf("async producer: %w", err)
	}

	outTopic := s.key.OutputTopic()
	var sent, failed int64

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

type chunkReader struct {
	idx    int
	file   *os.File
	reader *csv.Reader
	next   *data.Person
}

func newChunkReader(path string, idx int) (*chunkReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r := &chunkReader{idx: idx, file: f, reader: csv.NewReader(bufio.NewReaderSize(f, 32*1024))}
	r.reader.FieldsPerRecord = 4
	r.reader.ReuseRecord = true
	r.advance()
	return r, nil
}

func (r *chunkReader) peek() *data.Person { return r.next }

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
	r.next = &data.Person{
		ID:        int32(id64),
		Name:      strings.TrimSpace(fields[1]),
		Address:   strings.TrimSpace(fields[2]),
		Continent: strings.TrimSpace(fields[3]),
	}
}

func (r *chunkReader) close() { r.file.Close() }

// ─── Min-heap ─────────────────────────────────────────────────────────────────

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

// ─── Configs ──────────────────────────────────────────────────────────────────

// NewClientConfig is kept for verify.go which runs alone after all sorters.
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

// newParallelClientConfig: conservative fetch buffers for 3 concurrent sorters.
// 3 sorters × 3 partitions × 6 MB = 54 MB total fetch buffer (vs 450 MB before).
func newParallelClientConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_6_0_0
	cfg.Consumer.Fetch.Min = 1
	cfg.Consumer.Fetch.Default = 6 * 1024 * 1024
	cfg.Consumer.Fetch.Max = 12 * 1024 * 1024
	cfg.Consumer.MaxWaitTime = 500 * time.Millisecond
	cfg.Consumer.Retry.Backoff = 200 * time.Millisecond
	return cfg
}

func newOutputProducerConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_6_0_0
	cfg.Producer.RequiredAcks = sarama.WaitForLocal
	cfg.Producer.Compression = sarama.CompressionSnappy
	cfg.Producer.Flush.Messages = 100_000
	cfg.Producer.Flush.Frequency = 200 * time.Millisecond
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Retry.Backoff = 200 * time.Millisecond
	cfg.ChannelBufferSize = 10_000
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	return cfg
}
