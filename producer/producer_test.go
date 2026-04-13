package producer

import (
	"strings"
	"testing"
)

// ─── ParseCSVLine ─────────────────────────────────────────────────────────────

func TestParseCSVLine_Valid(t *testing.T) {
	cases := []struct {
		line     string
		wantID   int32
		wantName string
		wantAddr string
		wantCont string
	}{
		{"42,AliceName,12 Main St,Asia", 42, "AliceName", "12 Main St", "Asia"},
		{"0,BobbySmith,9 Elm Avenue,Europe", 0, "BobbySmith", "9 Elm Avenue", "Europe"},
		{"50000000,ZNameHere,1 Road Blvd,Africa", 50000000, "ZNameHere", "1 Road Blvd", "Africa"},
		{"7,NameIsHere,3 Blvd St Here,North America", 7, "NameIsHere", "3 Blvd St Here", "North America"},
		{"-1,NegIDName,5 Test Road,Australia", -1, "NegIDName", "5 Test Road", "Australia"},
	}

	for _, tc := range cases {
		p, err := ParseCSVLine(tc.line)
		if err != nil {
			t.Errorf("ParseCSVLine(%q) unexpected error: %v", tc.line, err)
			continue
		}
		if p.ID != tc.wantID {
			t.Errorf("ParseCSVLine(%q) ID = %d, want %d", tc.line, p.ID, tc.wantID)
		}
		if p.Name != tc.wantName {
			t.Errorf("ParseCSVLine(%q) Name = %q, want %q", tc.line, p.Name, tc.wantName)
		}
		if p.Address != tc.wantAddr {
			t.Errorf("ParseCSVLine(%q) Address = %q, want %q", tc.line, p.Address, tc.wantAddr)
		}
		if p.Continent != tc.wantCont {
			t.Errorf("ParseCSVLine(%q) Continent = %q, want %q", tc.line, p.Continent, tc.wantCont)
		}
	}
}

func TestParseCSVLine_TooFewFields(t *testing.T) {
	bad := []string{
		"",
		"42",
		"42,name",
		"42,name,address",
	}
	for _, line := range bad {
		_, err := ParseCSVLine(line)
		if err == nil {
			t.Errorf("ParseCSVLine(%q) expected error, got nil", line)
		}
	}
}

func TestParseCSVLine_InvalidID(t *testing.T) {
	cases := []string{
		"notanumber,Name,Address,Asia",
		"3.14,Name,Address,Asia",
		",Name,Address,Asia",
		"99999999999,Name,Address,Asia", // overflows int32
	}
	for _, line := range cases {
		_, err := ParseCSVLine(line)
		if err == nil {
			t.Errorf("ParseCSVLine(%q) expected error for bad id, got nil", line)
		}
	}
}

func TestParseCSVLine_WhitespaceTrimmed(t *testing.T) {
	p, err := ParseCSVLine("  7 , NameHere , 1 Road Blvd , Asia ")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.ID != 7 {
		t.Errorf("ID = %d, want 7", p.ID)
	}
	if p.Name != "NameHere" {
		t.Errorf("Name = %q, want %q", p.Name, "NameHere")
	}
	if p.Continent != "Asia" {
		t.Errorf("Continent = %q, want %q", p.Continent, "Asia")
	}
}

func TestParseCSVLine_ContinentWithSpaces(t *testing.T) {
	// "North America" and "South America" must not be split by SplitN.
	for _, cont := range []string{"North America", "South America"} {
		line := "1,NameHolder,5 Test Road," + cont
		p, err := ParseCSVLine(line)
		if err != nil {
			t.Fatalf("ParseCSVLine(%q): %v", line, err)
		}
		if p.Continent != cont {
			t.Errorf("Continent = %q, want %q", p.Continent, cont)
		}
	}
}

// ─── sendBatch filter ─────────────────────────────────────────────────────────

// sendBatch skips empty lines and lines with fewer than 3 commas.
// We mirror that filter here to verify the boundary conditions independently.
func TestSendBatchFilter_CommaCheck(t *testing.T) {
	check := func(line string) bool {
		line = strings.TrimSpace(line)
		return line != "" && strings.Count(line, ",") >= 3
	}

	valid := []string{
		"1,Name,Address,Continent",
		"0,NameHere,1 Road Blvd,Asia",
	}
	for _, line := range valid {
		if !check(line) {
			t.Errorf("valid line rejected: %q", line)
		}
	}

	invalid := []string{
		"",
		"   ",
		"nocommasatall",
		"one,comma",
		"two,commas,only",
	}
	for _, line := range invalid {
		if check(line) {
			t.Errorf("invalid line accepted: %q", line)
		}
	}
}

// ─── roundRobinPartitioner ────────────────────────────────────────────────────

func TestRoundRobinPartitioner_Distribution(t *testing.T) {
	rr := &roundRobinPartitioner{}
	counts := make(map[int32]int)
	const msgs = 3000
	const partitions = 3

	for i := 0; i < msgs; i++ {
		p, err := rr.Partition(nil, partitions)
		if err != nil {
			t.Fatalf("Partition() error: %v", err)
		}
		if p < 0 || p >= partitions {
			t.Errorf("partition %d out of range [0, %d)", p, partitions)
		}
		counts[p]++
	}

	for p, c := range counts {
		if c != msgs/partitions {
			t.Errorf("partition %d: got %d messages, want %d", p, c, msgs/partitions)
		}
	}
}

func TestRoundRobinPartitioner_RequiresConsistency(t *testing.T) {
	rr := &roundRobinPartitioner{}
	if rr.RequiresConsistency() {
		t.Error("RequiresConsistency() should return false for round-robin")
	}
}

func TestRoundRobinPartitioner_SinglePartition(t *testing.T) {
	rr := &roundRobinPartitioner{}
	for i := 0; i < 100; i++ {
		p, err := rr.Partition(nil, 1)
		if err != nil {
			t.Fatalf("Partition() error: %v", err)
		}
		if p != 0 {
			t.Errorf("single partition: got %d, want 0", p)
		}
	}
}

func TestRoundRobinPartitioner_Wraparound(t *testing.T) {
	rr := &roundRobinPartitioner{}
	const partitions = int32(3)
	const rounds = 5
	results := make([]int32, rounds*int(partitions))

	for i := range results {
		p, _ := rr.Partition(nil, partitions)
		results[i] = p
	}

	// Verify the first two full rounds are 0,1,2 each.
	for round := 0; round < 2; round++ {
		for i := int32(0); i < partitions; i++ {
			got := results[round*int(partitions)+int(i)]
			if got != i {
				t.Errorf("round %d, slot %d: got partition %d, want %d", round, i, got, i)
			}
		}
	}
}
