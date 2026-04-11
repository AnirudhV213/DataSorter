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
	// Cannot use 1M here — all three sorters allocate their chunk arrays
	// simultaneously, and combined with Kafka fetch buffers this OOM-kills
	// the process against the 1 GB Go app limit.
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
// Memory budget (3 sorters in parallel):
//
//	Kafka JVM (pinned):               512 MB
//	3 × chunk array (500K × 130B):    195 MB
//	3 × msgCh buffer (50K × 130B):     20 MB
//	3 × fetch buffers (6MB × 3 parts):  54 MB
//	Go runtime + misc:                 ~50 MB
//	                                  --------
//	Total peak:                        ~831 MB  ← within 1 GB app limit
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

func (s *Sorter) Run(ctx context.Context) error {
	start := time.Now()
	fmt.Printf("[%s] starting (external merge sort)\n", s.key)

	defer func() {
		os.RemoveAll(s.tmpDir)
		fmt.Printf("[%s] temp dir cleaned up\n", s.key)
	}()

	if err := producer.EnsureTopic(s.brokers, s.key.OutputTopic(), outputPartitions); err != nil {
		return fmt.Errorf("[%s] ensure topic: %w", s.key, err)
	}

	t1 := time.Now()
	chunkFiles, totalRecords, err := s.chunkSort(ctx)
	if err != nil {
		return fmt.Errorf("[%s] chunk sort: %w", s.key, err)
	}
	fmt.Printf("[%s] phase1 chunk-sort: %d records → %d chunk files in %.2fs\n",
		s.key, totalRecords, len(chunkFiles), time.Since(t1).Seconds())

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

func (s *Sorter) chunkSort(ctx context.Context) ([]string, int64, error) {
	// Use the memory-conscious parallel config, not the original high-fetch config.
	client, err := sarama.NewClient(s.brokers, newParallelClientConfig())
	if err != nil {
		return nil, 0, fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	partitions, err := client.Partitions(producer.TopicSource)
	if err != nil {
		return nil, 0, fmt.Errorf("list partitions: %w", err)
	}

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

	// 50K buffer × 130 B = 6.5 MB per sorter (3 sorters = ~20 MB total).
	// Original was chunkSize/10 = 100K; halved to stay within memory budget.
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

	go func() {
		<-fanInDone
		close(msgCh)
	}()

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
	if err := flushChunk(); err != nil {
		return nil, 0, err
	}

	return chunkFiles, totalRecords, nil
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

// ─── Phase 2: k-way merge ────────────────────────────────────────────────────

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

// NewClientConfig is kept for verify.go which runs alone after all sorters
// complete — fetch buffers don't compete with anything at that point.
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

// newParallelClientConfig is used during parallel sorter execution.
// Fetch buffers are sized so that 3 sorters × 3 partitions fits within budget:
//
//	3 × 3 partitions × 6 MB default = 54 MB   (was 10 MB default → 90 MB,
//	and Fetch.Max 50 MB meant spikes up to 450 MB — the OOM trigger).
func newParallelClientConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_6_0_0
	cfg.Consumer.Fetch.Min = 1
	cfg.Consumer.Fetch.Default = 6 * 1024 * 1024 // 6 MB  (was 10 MB)
	cfg.Consumer.Fetch.Max = 12 * 1024 * 1024    // 12 MB (was 50 MB)
	cfg.Consumer.MaxWaitTime = 500 * time.Millisecond
	cfg.Consumer.Retry.Backoff = 200 * time.Millisecond
	return cfg
}

func newOutputProducerConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_6_0_0
	// WaitForLocal: single broker + RF=1 means WaitForAll adds latency with
	// zero durability benefit. Critical fix for the continent sorter's
	// 1328s merge phase — it was stalling waiting for unnecessary ISR acks.
	cfg.Producer.RequiredAcks = sarama.WaitForLocal
	cfg.Producer.Compression = sarama.CompressionSnappy
	cfg.Producer.Flush.Messages = 100_000
	cfg.Producer.Flush.Frequency = 200 * time.Millisecond
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Retry.Backoff = 200 * time.Millisecond
	cfg.ChannelBufferSize = 2_000_000
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	return cfg
}
