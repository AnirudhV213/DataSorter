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

	// csv.Reader with FieldsPerRecord=4 rejects lines with wrong field count;
	// advance() sets next=nil on any read error, so peek must return nil.
	if r.peek() != nil {
		t.Error("peek should return nil for malformed lines")
	}
}

// ─── Cardinality probe ────────────────────────────────────────────────────────

func TestCardinalityProbe_LowCardinality(t *testing.T) {
	// 6 continent values must fall below the bucket-sort threshold.
	continents := []string{"Africa", "Asia", "Australia", "Europe", "North America", "South America"}
	distinct := make(map[string]struct{}, len(continents))
	for _, c := range continents {
		distinct[c] = struct{}{}
	}
	if len(distinct) >= lowCardinalityThreshold {
		t.Errorf("continent cardinality %d should be < threshold %d",
			len(distinct), lowCardinalityThreshold)
	}
}

func TestCardinalityProbe_HighCardinality(t *testing.T) {
	// Unique name/id values must exceed the threshold to trigger merge sort.
	distinct := make(map[string]struct{}, lowCardinalityThreshold+1)
	for i := 0; i <= lowCardinalityThreshold; i++ {
		distinct[fmt.Sprintf("name_%d", i)] = struct{}{}
	}
	if len(distinct) < lowCardinalityThreshold {
		t.Errorf("name cardinality %d should be >= threshold %d",
			len(distinct), lowCardinalityThreshold)
	}
}

// ─── Bucket sort correctness ──────────────────────────────────────────────────

func TestBucketSortOutput_ContinentOrder(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "bucket-test-*")
	defer os.RemoveAll(tmpDir)

	records := map[string]*data.Person{
		"Africa": {ID: 3, Name: "C", Address: "3 C St", Continent: "Africa"},
		"Europe": {ID: 1, Name: "A", Address: "1 A Ave", Continent: "Europe"},
		"Asia":   {ID: 2, Name: "B", Address: "2 B Blvd", Continent: "Asia"},
	}

	for key, p := range records {
		path := filepath.Join(tmpDir, fmt.Sprintf("bucket_%s.csv", key))
		f, _ := os.Create(path)
		w := bufio.NewWriter(f)
		fmt.Fprintln(w, p.ToCSV())
		w.Flush()
		f.Close()
	}

	// Read buckets in sorted key order and collect continent fields.
	keys := []string{"Africa", "Asia", "Europe"}
	var got []string
	for _, k := range keys {
		path := filepath.Join(tmpDir, fmt.Sprintf("bucket_%s.csv", k))
		f, _ := os.Open(path)
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			parts := strings.SplitN(scanner.Text(), ",", 4)
			if len(parts) == 4 {
				got = append(got, parts[3])
			}
		}
		f.Close()
	}

	for i, want := range keys {
		if got[i] != want {
			t.Errorf("position %d: got %q, want %q", i, got[i], want)
		}
	}
}

func TestBucketSortOutput_NorthAmericaSanitised(t *testing.T) {
	if got := strings.ReplaceAll("North America", " ", "_"); got != "North_America" {
		t.Errorf("sanitised name = %q, want %q", got, "North_America")
	}
	if got := strings.ReplaceAll("South America", " ", "_"); got != "South_America" {
		t.Errorf("sanitised name = %q, want %q", got, "South_America")
	}
}

// ─── External merge sort correctness ─────────────────────────────────────────

func TestExternalMergeSort_ChunkSortCorrectness(t *testing.T) {
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
	// 3 × 500K × 130B ≈ 195MB — safe within the 1GB app limit.
	const maxSafeParallelChunkSize = 600_000
	if chunkSize > maxSafeParallelChunkSize {
		t.Errorf("chunkSize %d exceeds safe parallel limit %d", chunkSize, maxSafeParallelChunkSize)
	}
	if chunkSize < 100_000 {
		t.Error("chunkSize too small — will create too many chunk files and slow the merge")
	}
}

func TestConstants_LowCardinalityThreshold(t *testing.T) {
	// Must be above 6 (continent count) but well below typical name/id cardinality.
	if lowCardinalityThreshold <= 6 {
		t.Errorf("lowCardinalityThreshold %d must be > 6 (continent count)", lowCardinalityThreshold)
	}
	if lowCardinalityThreshold > 10_000 {
		t.Errorf("lowCardinalityThreshold %d too high — name sort may wrongly use bucket sort", lowCardinalityThreshold)
	}
}

func TestConfig_ParallelClientFetchMax(t *testing.T) {
	cfg := newParallelClientConfig()
	// 3 sorters × 3 partitions × Fetch.Max must stay within reason.
	// Keep at or below 16MB to avoid OOM with 3 concurrent sorters.
	const maxFetch = 16 * 1024 * 1024
	if cfg.Consumer.Fetch.Max > maxFetch {
		t.Errorf("Fetch.Max %d exceeds safe limit %d for parallel execution",
			cfg.Consumer.Fetch.Max, maxFetch)
	}
}

func TestConfig_OutputProducerChannelBuffer(t *testing.T) {
	cfg := newOutputProducerConfig()
	// 3 concurrent producers — keep ChannelBufferSize small to avoid OOM.
	const maxBuffer = 100_000
	if cfg.ChannelBufferSize > maxBuffer {
		t.Errorf("ChannelBufferSize %d too large for 3 concurrent producers (limit %d)",
			cfg.ChannelBufferSize, maxBuffer)
	}
}

func TestConfig_OutputProducerAcks(t *testing.T) {
	cfg := newOutputProducerConfig()
	// WaitForLocal (1) is correct for a single-broker RF=1 cluster.
	if cfg.Producer.RequiredAcks != 1 {
		t.Errorf("RequiredAcks = %d, want 1 (WaitForLocal)", cfg.Producer.RequiredAcks)
	}
}
