package data

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"testing"
)

// ─── Person ───────────────────────────────────────────────────────────────────

func TestPersonToCSV(t *testing.T) {
	p := NewPerson(42, "Alice", "12 Main St", "Asia")
	got := p.ToCSV()
	want := "42,Alice,12 Main St,Asia"
	if got != want {
		t.Errorf("ToCSV() = %q, want %q", got, want)
	}
}

func TestPersonToCSV_NegativeID(t *testing.T) {
	p := NewPerson(-1, "Bob", "99 Elm Ave", "Europe")
	got := p.ToCSV()
	if !strings.HasPrefix(got, "-1,") {
		t.Errorf("ToCSV() should preserve negative id, got %q", got)
	}
}

func TestPersonToCSV_RoundTrip(t *testing.T) {
	p := NewPerson(123, "TestName", "5 Road Blvd", "Africa")
	csv := p.ToCSV()
	parts := strings.SplitN(csv, ",", 4)
	if len(parts) != 4 {
		t.Fatalf("ToCSV() produced %d fields, want 4", len(parts))
	}
	id, err := strconv.ParseInt(parts[0], 10, 32)
	if err != nil {
		t.Fatalf("id field not parseable: %v", err)
	}
	if int32(id) != p.ID || parts[1] != p.Name || parts[2] != p.Address || parts[3] != p.Continent {
		t.Errorf("round-trip mismatch: got %v", parts)
	}
}

// ─── Generator ────────────────────────────────────────────────────────────────

var validContinents = map[string]bool{
	"North America": true,
	"Asia":          true,
	"South America": true,
	"Europe":        true,
	"Africa":        true,
	"Australia":     true,
}

func TestGeneratorContinent(t *testing.T) {
	g := NewGenerator()
	for i := 0; i < 1000; i++ {
		p := g.GeneratePerson()
		if !validContinents[p.Continent] {
			t.Errorf("invalid continent %q at iteration %d", p.Continent, i)
		}
	}
}

func TestGeneratorNameLength(t *testing.T) {
	g := NewGenerator()
	for i := 0; i < 1000; i++ {
		p := g.GeneratePerson()
		l := len(p.Name)
		if l < 10 || l > 15 {
			t.Errorf("name length %d out of [10,15]: %q", l, p.Name)
		}
	}
}

func TestGeneratorNameCharset(t *testing.T) {
	g := NewGenerator()
	for i := 0; i < 1000; i++ {
		p := g.GeneratePerson()
		for _, c := range p.Name {
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
				t.Errorf("name %q contains non-letter char %q", p.Name, c)
				break
			}
		}
	}
}

func TestGeneratorAddressLength(t *testing.T) {
	g := NewGenerator()
	for i := 0; i < 1000; i++ {
		p := g.GeneratePerson()
		l := len(p.Address)
		if l < 15 || l > 20 {
			t.Errorf("address length %d out of [15,20]: %q", l, p.Address)
		}
	}
}

func TestGeneratorAddressHasDigitAndSpace(t *testing.T) {
	g := NewGenerator()
	for i := 0; i < 500; i++ {
		p := g.GeneratePerson()
		if p.Address[0] < '0' || p.Address[0] > '9' {
			t.Errorf("address[0] not a digit: %q", p.Address)
		}
		if p.Address[2] != ' ' {
			t.Errorf("address[2] not a space: %q", p.Address)
		}
	}
}

func TestGeneratorIDRange(t *testing.T) {
	g := NewGenerator()
	for i := 0; i < 10000; i++ {
		p := g.GeneratePerson()
		if p.ID < 0 || p.ID >= 50_000_000 {
			t.Errorf("id %d out of expected range [0, 50000000)", p.ID)
		}
	}
}

func TestGeneratorProducesDistinctRecords(t *testing.T) {
	g := NewGenerator()
	seen := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		p := g.GeneratePerson()
		key := p.ToCSV()
		if seen[key] {
			t.Logf("duplicate record (name+address collisions are possible): %q", key)
		}
		seen[key] = true
	}
}

// ─── GenerateToCSV ────────────────────────────────────────────────────────────

func TestGenerateToCSV_RecordCount(t *testing.T) {
	f, err := os.CreateTemp("", "datagen-*.csv")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	const n = 1000
	if err := GenerateToCSV(path, n); err != nil {
		t.Fatalf("GenerateToCSV: %v", err)
	}

	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lines := 0
	for scanner.Scan() {
		lines++
	}
	// 1 header + n data lines.
	if lines != n+1 {
		t.Errorf("line count = %d, want %d", lines, n+1)
	}
}

func TestGenerateToCSV_Header(t *testing.T) {
	f, _ := os.CreateTemp("", "datagen-*.csv")
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	if err := GenerateToCSV(path, 10); err != nil {
		t.Fatal(err)
	}

	file, _ := os.Open(path)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Scan()
	if got := scanner.Text(); got != "id,name,address,continent" {
		t.Errorf("header = %q, want %q", got, "id,name,address,continent")
	}
}

func TestGenerateToCSV_FieldCount(t *testing.T) {
	f, _ := os.CreateTemp("", "datagen-*.csv")
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	if err := GenerateToCSV(path, 100); err != nil {
		t.Fatal(err)
	}

	file, _ := os.Open(path)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Scan() // skip header
	lineNum := 1
	for scanner.Scan() {
		lineNum++
		fields := strings.SplitN(scanner.Text(), ",", 4)
		if len(fields) != 4 {
			t.Errorf("line %d: got %d fields, want 4: %q", lineNum, len(fields), scanner.Text())
		}
	}
}

func TestGenerateToCSV_ValidContinents(t *testing.T) {
	f, _ := os.CreateTemp("", "datagen-*.csv")
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	if err := GenerateToCSV(path, 200); err != nil {
		t.Fatal(err)
	}

	file, _ := os.Open(path)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Scan() // skip header
	for scanner.Scan() {
		fields := strings.SplitN(scanner.Text(), ",", 4)
		if len(fields) < 4 {
			continue
		}
		if !validContinents[fields[3]] {
			t.Errorf("invalid continent %q in line %q", fields[3], scanner.Text())
		}
	}
}

func TestGenerateToCSV_ZeroRecords(t *testing.T) {
	f, _ := os.CreateTemp("", "datagen-*.csv")
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	if err := GenerateToCSV(path, 0); err != nil {
		t.Fatalf("GenerateToCSV(0): %v", err)
	}

	file, _ := os.Open(path)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lines := 0
	for scanner.Scan() {
		lines++
	}
	if lines != 1 {
		t.Errorf("0 records: expected 1 line (header), got %d", lines)
	}
}

func TestGenerateToCSV_InvalidPath(t *testing.T) {
	err := GenerateToCSV("/nonexistent/dir/file.csv", 10)
	if err == nil {
		t.Error("expected error for invalid path, got nil")
	}
}
