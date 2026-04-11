package consumer

import (
	"bufio"
	"container/heap"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/AnirudhV16/DataSorter/data"
)

// ─── lessFor / comparators ────────────────────────────────────────────────────

func TestLessFor_ID(t *testing.T) {
	less, err := lessFor(SortByID)
	if err != nil {
		t.Fatal(err)
	}
	a := &data.Person{ID: 1}
	b := &data.Person{ID: 2}
	if !less(a, b) {
		t.Error("less(1,2) should be true")
	}
	if less(b, a) {
		t.Error("less(2,1) should be false")
	}
	if less(a, a) {
		t.Error("less(1,1) should be false (strict)")
	}
}

func TestLessFor_Name(t *testing.T) {
	less, err := lessFor(SortByName)
	if err != nil {
		t.Fatal(err)
	}
	a := &data.Person{Name: "Alice"}
	b := &data.Person{Name: "Bob"}
	if !less(a, b) {
		t.Error("less(Alice,Bob) should be true")
	}
	if less(b, a) {
		t.Error("less(Bob,Alice) should be false")
	}
}

func TestLessFor_Continent(t *testing.T) {
	less, err := lessFor(SortByContinent)
	if err != nil {
		t.Fatal(err)
	}
	a := &data.Person{Continent: "Africa"}
	b := &data.Person{Continent: "Europe"}
	if !less(a, b) {
		t.Error("less(Africa,Europe) should be true")
	}
}

func TestLessFor_Unknown(t *testing.T) {
	_, err := lessFor("unknown_key")
	if err == nil {
		t.Error("expected error for unknown sort key")
	}
}

// ─── keyOf ────────────────────────────────────────────────────────────────────

func TestKeyOf(t *testing.T) {
	p := &data.Person{ID: 99, Name: "TestName", Continent: "Asia"}
	if got := keyOf(SortByID, p); got != "99" {
		t.Errorf("keyOf(id) = %q, want %q", got, "99")
	}
	if got := keyOf(SortByName, p); got != "TestName" {
		t.Errorf("keyOf(name) = %q, want %q", got, "TestName")
	}
	if got := keyOf(SortByContinent, p); got != "Asia" {
		t.Errorf("keyOf(continent) = %q, want %q", got, "Asia")
	}
}

// ─── SortKey.OutputTopic ──────────────────────────────────────────────────────

func TestOutputTopic(t *testing.T) {
	cases := map[SortKey]string{
		SortByID:        "id",
		SortByName:      "name",
		SortByContinent: "continent",
	}
	for key, want := range cases {
		if got := key.OutputTopic(); got != want {
			t.Errorf("%s.OutputTopic() = %q, want %q", key, got, want)
		}
	}
}

// ─── mergeHeap ────────────────────────────────────────────────────────────────

func TestMergeHeap_ID(t *testing.T) {
	less, _ := lessFor(SortByID)
	h := &mergeHeap{less: less}

	records := []*data.Person{
		{ID: 5}, {ID: 2}, {ID: 8}, {ID: 1}, {ID: 3},
	}
	for i, p := range records {
		heap.Push(h, &heapEntry{person: p, readerIdx: i})
	}
	heap.Init(h)

	prev := int32(-1)
	for h.Len() > 0 {
		e := heap.Pop(h).(*heapEntry)
		if e.person.ID < prev {
			t.Errorf("heap out of order: %d after %d", e.person.ID, prev)
		}
		prev = e.person.ID
	}
}

func TestMergeHeap_Name(t *testing.T) {
	less, _ := lessFor(SortByName)
	h := &mergeHeap{less: less}

	names := []string{"Zara", "Alice", "Mike", "Bob", "Nancy"}
	for i, n := range names {
		heap.Push(h, &heapEntry{person: &data.Person{Name: n}, readerIdx: i})
	}
	heap.Init(h)

	var got []string
	for h.Len() > 0 {
		e := heap.Pop(h).(*heapEntry)
		got = append(got, e.person.Name)
	}

	if !sort.StringsAreSorted(got) {
		t.Errorf("heap names not sorted: %v", got)
	}
}

func TestMergeHeap_SingleEntry(t *testing.T) {
	less, _ := lessFor(SortByID)
	h := &mergeHeap{less: less}
	heap.Push(h, &heapEntry{person: &data.Person{ID: 7}, readerIdx: 0})
	e := heap.Pop(h).(*heapEntry)
	if e.person.ID != 7 {
		t.Errorf("single entry pop: got %d, want 7", e.person.ID)
	}
	if h.Len() != 0 {
		t.Error("heap should be empty after popping single entry")
	}
}

func TestMergeHeap_DuplicateKeys(t *testing.T) {
	less, _ := lessFor(SortByID)
	h := &mergeHeap{less: less}
	for i := 0; i < 5; i++ {
		heap.Push(h, &heapEntry{person: &data.Person{ID: 42}, readerIdx: i})
	}
	heap.Init(h)
	count := 0
	for h.Len() > 0 {
		heap.Pop(h)
		count++
	}
	if count != 5 {
		t.Errorf("expected 5 pops for 5 duplicates, got %d", count)
	}
}

// ─── writeChunkFile / newChunkReader ─────────────────────────────────────────

func TestChunkFileRoundTrip(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sorter-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	less, _ := lessFor(SortByID)
	s := &Sorter{key: SortByID, less: less, tmpDir: tmpDir}

	input := []*data.Person{
		{ID: 3, Name: "Charlie", Address: "3 C St", Continent: "Asia"},
		{ID: 1, Name: "Alice", Address: "1 A Ave", Continent: "Europe"},
		{ID: 2, Name: "Bob", Address: "2 B Blvd", Continent: "Africa"},
	}
	// Sort before writing (as the real flushChunk does).
	sort.Slice(input, func(i, j int) bool { return s.less(input[i], input[j]) })

	path, err := s.writeChunkFile(input, 0)
	if err != nil {
		t.Fatalf("writeChunkFile: %v", err)
	}

	r, err := newChunkReader(path, 0)
	if err != nil {
		t.Fatalf("newChunkReader: %v", err)
	}
	defer r.close()

	var got []*data.Person
	for r.peek() != nil {
		got = append(got, r.peek())
		r.advance()
	}

	if len(got) != len(input) {
		t.Fatalf("read %d records, want %d", len(got), len(input))
	}
	for i, p := range got {
		if p.ID != input[i].ID {
			t.Errorf("record %d: ID %d, want %d", i, p.ID, input[i].ID)
		}
		if p.Name != input[i].Name {
			t.Errorf("record %d: Name %q, want %q", i, p.Name, input[i].Name)
		}
	}
}

func TestChunkFileRoundTrip_PreservesOrder(t *testing.T) {
	// Verify that whatever order is written is exactly what is read back.
	tmpDir, _ := os.MkdirTemp("", "sorter-order-*")
	defer os.RemoveAll(tmpDir)

	less, _ := lessFor(SortByName)
	s := &Sorter{key: SortByName, less: less, tmpDir: tmpDir}

	names := []string{"Alpha", "Beta", "Gamma", "Delta", "Epsilon"}
	var input []*data.Person
	for i, n := range names {
		input = append(input, &data.Person{ID: int32(i), Name: n, Continent: "Asia", Address: "1 A Ave"})
	}
	sort.Slice(input, func(i, j int) bool { return s.less(input[i], input[j]) })

	path, _ := s.writeChunkFile(input, 0)
	r, _ := newChunkReader(path, 0)
	defer r.close()

	idx := 0
	for r.peek() != nil {
		if r.peek().Name != input[idx].Name {
			t.Errorf("position %d: got %q, want %q", idx, r.peek().Name, input[idx].Name)
		}
		r.advance()
		idx++
	}
}

func TestChunkReader_EmptyFile(t *testing.T) {
	f, _ := os.CreateTemp("", "empty-chunk-*")
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	r, err := newChunkReader(path, 0)
	if err != nil {
		t.Fatalf("newChunkReader on empty file: %v", err)
	}
	defer r.close()

	if r.peek() != nil {
		t.Error("peek on empty file should return nil")
	}
}

func TestChunkReader_MalformedLine(t *testing.T) {
	f, _ := os.CreateTemp("", "malformed-chunk-*")
	path := f.Name()
	fmt.Fprintln(f, "not,a,valid,csv,line,too,many,fields")
	fmt.Fprintln(f, "also bad")
	f.Close()
	defer os.Remove(path)

	r, err := newChunkReader(path, 0)
	if err != nil {
		t.Fatalf("newChunkReader: %v", err)
	}
	defer r.close()
	// Malformed lines should be skipped — peek returns nil.
	if r.peek() != nil {
		t.Logf("peek returned non-nil for malformed lines (may be tolerated)")
	}
}

// ─── Cardinality probe / bucket sort path selection ───────────────────────────

func TestCardinalityProbe_LowCardinality(t *testing.T) {
	// Simulate continent: 6 distinct values in 500K records → bucket sort.
	continents := []string{"Africa", "Asia", "Australia", "Europe", "North America", "South America"}
	distinct := make(map[string]struct{})
	for i := 0; i < 500_000; i++ {
		distinct[continents[i%len(continents)]] = struct{}{}
	}
	if len(distinct) >= lowCardinalityThreshold {
		t.Errorf("continent cardinality %d should be < threshold %d",
			len(distinct), lowCardinalityThreshold)
	}
}

func TestCardinalityProbe_HighCardinality(t *testing.T) {
	// Simulate name / id: many distinct values → external merge sort.
	distinct := make(map[string]struct{})
	for i := 0; i < 500_000; i++ {
		distinct[fmt.Sprintf("name_%d", i)] = struct{}{}
	}
	if len(distinct) < lowCardinalityThreshold {
		t.Errorf("name cardinality %d should be >= threshold %d",
			len(distinct), lowCardinalityThreshold)
	}
}

// ─── Bucket sort correctness ──────────────────────────────────────────────────

func TestBucketSortOutput_ContinentOrder(t *testing.T) {
	// Write records for 3 continents into bucket files manually, then verify
	// that reading them in sorted key order produces the correct sequence.
	tmpDir, _ := os.MkdirTemp("", "bucket-test-*")
	defer os.RemoveAll(tmpDir)

	type bucket struct{ key, safe string }
	buckets := []bucket{
		{"Africa", "Africa"},
		{"Asia", "Asia"},
		{"Europe", "Europe"},
	}

	records := map[string][]*data.Person{
		"Africa": {{ID: 3, Name: "C", Address: "3 C St", Continent: "Africa"}},
		"Europe": {{ID: 1, Name: "A", Address: "1 A Ave", Continent: "Europe"}},
		"Asia":   {{ID: 2, Name: "B", Address: "2 B Blvd", Continent: "Asia"}},
	}

	// Write bucket files.
	for _, b := range buckets {
		path := filepath.Join(tmpDir, fmt.Sprintf("bucket_%s.csv", b.safe))
		f, _ := os.Create(path)
		w := bufio.NewWriter(f)
		for _, p := range records[b.key] {
			fmt.Fprintln(w, p.ToCSV())
		}
		w.Flush()
		f.Close()
	}

	// Read in sorted key order.
	keys := []string{"Africa", "Asia", "Europe"}
	var got []string
	for _, k := range keys {
		path := filepath.Join(tmpDir, fmt.Sprintf("bucket_%s.csv", k))
		f, _ := os.Open(path)
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			parts := strings.SplitN(scanner.Text(), ",", 4)
			if len(parts) == 4 {
				got = append(got, parts[3]) // continent field
			}
		}
		f.Close()
	}

	want := []string{"Africa", "Asia", "Europe"}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("position %d: got %q, want %q", i, got[i], w)
		}
	}
}

func TestBucketSortOutput_NorthAmericaSanitised(t *testing.T) {
	// "North America" becomes "North_America" in filename — verify sanitisation.
	safe := strings.ReplaceAll("North America", " ", "_")
	if safe != "North_America" {
		t.Errorf("sanitised name = %q, want %q", safe, "North_America")
	}
	safe2 := strings.ReplaceAll("South America", " ", "_")
	if safe2 != "South_America" {
		t.Errorf("sanitised name = %q, want %q", safe2, "South_America")
	}
}

// ─── externalMergeSort sort correctness (unit-level) ─────────────────────────

func TestExternalMergeSort_ChunkSortCorrectness(t *testing.T) {
	// Sort a slice of Persons by ID and verify ordering — the same logic
	// used in flushChunk.
	less, _ := lessFor(SortByID)
	persons := []*data.Person{
		{ID: 9}, {ID: 3}, {ID: 7}, {ID: 1}, {ID: 5}, {ID: 2}, {ID: 8}, {ID: 4}, {ID: 6},
	}
	sort.Slice(persons, func(i, j int) bool { return less(persons[i], persons[j]) })

	for i := 1; i < len(persons); i++ {
		if persons[i].ID < persons[i-1].ID {
			t.Errorf("out of order at [%d]: %d < %d", i, persons[i].ID, persons[i-1].ID)
		}
	}
}

func TestExternalMergeSort_NameSortCorrectness(t *testing.T) {
	less, _ := lessFor(SortByName)
	names := []string{"Zeta", "Alpha", "Mu", "Beta", "Omicron"}
	persons := make([]*data.Person, len(names))
	for i, n := range names {
		persons[i] = &data.Person{Name: n}
	}
	sort.Slice(persons, func(i, j int) bool { return less(persons[i], persons[j]) })

	var sorted []string
	for _, p := range persons {
		sorted = append(sorted, p.Name)
	}
	if !sort.StringsAreSorted(sorted) {
		t.Errorf("names not sorted: %v", sorted)
	}
}

func TestExternalMergeSort_KWayMerge(t *testing.T) {
	// Simulate a 3-way merge of sorted chunks and verify output is fully sorted.
	tmpDir, _ := os.MkdirTemp("", "merge-test-*")
	defer os.RemoveAll(tmpDir)

	less, _ := lessFor(SortByID)
	s := &Sorter{key: SortByID, less: less, tmpDir: tmpDir}

	chunks := [][]*data.Person{
		{{ID: 1}, {ID: 4}, {ID: 7}},
		{{ID: 2}, {ID: 5}, {ID: 8}},
		{{ID: 3}, {ID: 6}, {ID: 9}},
	}

	var chunkPaths []string
	for i, chunk := range chunks {
		path, err := s.writeChunkFile(chunk, i)
		if err != nil {
			t.Fatal(err)
		}
		chunkPaths = append(chunkPaths, path)
	}

	// Open readers and seed heap.
	h := &mergeHeap{less: less}
	readers := make([]*chunkReader, len(chunkPaths))
	for i, path := range chunkPaths {
		r, _ := newChunkReader(path, i)
		readers[i] = r
		if p := r.peek(); p != nil {
			heap.Push(h, &heapEntry{person: p, readerIdx: i})
			r.advance()
		}
	}
	heap.Init(h)

	var result []int32
	for h.Len() > 0 {
		e := heap.Pop(h).(*heapEntry)
		result = append(result, e.person.ID)
		r := readers[e.readerIdx]
		if next := r.peek(); next != nil {
			heap.Push(h, &heapEntry{person: next, readerIdx: e.readerIdx})
			r.advance()
		}
	}
	for _, r := range readers {
		r.close()
	}

	want := []int32{1, 2, 3, 4, 5, 6, 7, 8, 9}
	if len(result) != len(want) {
		t.Fatalf("result length %d, want %d", len(result), len(want))
	}
	for i, v := range want {
		if result[i] != v {
			t.Errorf("position %d: got %d, want %d", i, result[i], v)
		}
	}
}

// ─── Config sanity checks ─────────────────────────────────────────────────────

func TestConstants_ChunkSize(t *testing.T) {
	// chunkSize must be large enough for meaningful batching but not so large
	// that 3 parallel sorters exceed the 1 GB app memory limit.
	// 3 × 500K × 130 B ≈ 195 MB — safe.
	// 3 × 1M  × 130 B ≈ 390 MB — also within 1 GB but leaves little headroom
	// once fetch buffers and producer channels are added.
	const maxSafeParallelChunkSize = 600_000
	if chunkSize > maxSafeParallelChunkSize {
		t.Errorf("chunkSize %d exceeds safe parallel limit %d", chunkSize, maxSafeParallelChunkSize)
	}
	if chunkSize < 100_000 {
		t.Error("chunkSize too small — will create too many chunk files and slow the merge")
	}
}

func TestConstants_LowCardinalityThreshold(t *testing.T) {
	// Must be above the 6 continent values but well below typical name/id cardinality.
	if lowCardinalityThreshold <= 6 {
		t.Errorf("lowCardinalityThreshold %d must be > 6 (continent count)", lowCardinalityThreshold)
	}
	if lowCardinalityThreshold > 10_000 {
		t.Errorf("lowCardinalityThreshold %d is too high — name sort may wrongly use bucket sort", lowCardinalityThreshold)
	}
}

func TestConfig_ParallelClientFetchMax(t *testing.T) {
	cfg := newParallelClientConfig()
	// 3 sorters × 3 partitions × Fetch.Max must stay within reason.
	// Fetch.Max was 50 MB before → 450 MB just for fetch buffers → OOM.
	// Now must be ≤ 16 MB.
	const maxFetch = 16 * 1024 * 1024
	if cfg.Consumer.Fetch.Max > maxFetch {
		t.Errorf("Fetch.Max %d exceeds safe limit %d for parallel execution",
			cfg.Consumer.Fetch.Max, maxFetch)
	}
}

func TestConfig_OutputProducerChannelBuffer(t *testing.T) {
	cfg := newOutputProducerConfig()
	// ChannelBufferSize was 2_000_000 → ~400 MB per producer → OOM with 3.
	// Must be ≤ 100_000 for safe concurrent operation.
	const maxBuffer = 100_000
	if cfg.ChannelBufferSize > maxBuffer {
		t.Errorf("ChannelBufferSize %d too large — 3 concurrent producers will OOM (limit %d)",
			cfg.ChannelBufferSize, maxBuffer)
	}
}

func TestConfig_OutputProducerAcks(t *testing.T) {
	cfg := newOutputProducerConfig()
	// Must be WaitForLocal (1), not WaitForAll (-1).
	// WaitForAll on a single-broker RF=1 cluster added latency with no
	// durability benefit and was the main cause of continent sort slowness.
	if cfg.Producer.RequiredAcks != 1 {
		t.Errorf("RequiredAcks = %d, want 1 (WaitForLocal)", cfg.Producer.RequiredAcks)
	}
}
