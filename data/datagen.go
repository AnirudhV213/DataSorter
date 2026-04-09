package data

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"
)

// id (int32) | name (string) | address(string) | Continent (string) |

// Schema: id (int32) | name (string) | address (string) | continent (string)

// continents holds all valid continent values as defined in the schema.
var continents = []string{
	"North America",
	"Asia",
	"South America",
	"Europe",
	"Africa",
	"Australia",
}

// letters contains only English alphabet characters, used for name generation.
const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// alphaNumSpace contains characters used for address generation:
// digits, uppercase/lowercase letters, and spaces.
const alphaNumSpace = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "

// Person represents a single record matching the pipeline schema.
type Person struct {
	ID        int32
	Name      string
	Address   string
	Continent string
}

// NewPerson constructs a Person with the given fields.
func NewPerson(id int32, name, address, continent string) *Person {
	return &Person{
		ID:        id,
		Name:      name,
		Address:   address,
		Continent: continent,
	}
}

// ToCSV serialises the Person into a comma-separated line (no trailing newline).
// Format: id,name,address,continent
func (p *Person) ToCSV() string {
	return fmt.Sprintf("%d,%s,%s,%s", p.ID, p.Name, p.Address, p.Continent)
}

// Generator holds configuration and a seeded random source for data generation.
type Generator struct {
	flag int
	rng  *rand.Rand
}

// NewGenerator creates a Generator seeded from the current wall-clock time.
func NewGenerator() *Generator {
	return &Generator{
		flag: 1,
		rng:  rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// randomID returns a random non-zero int32 value.
// The schema specifies "integer number within 32-bit range", so we use the full
// positive int32 range [1, 2^31-1] to avoid confusing a zero-ID with "no ID".
/*func (g *Generator) randomID() int32 {
	// rand.Int31() returns [0, 2^31-1]; add 1 to exclude 0.
	return g.rng.Int31() + 1
}*/

// randomString builds a random string of length in [minLen, maxLen] using only
// characters drawn from the provided charset.
//
// Algorithm: pick a length uniformly, then fill a byte slice by indexing into
// the charset at random positions. This is O(length) with no allocations beyond
// the result slice.
func (g *Generator) randomString(charset string, minLen, maxLen int) string {
	length := minLen + g.rng.Intn(maxLen-minLen+1)
	buf := make([]byte, length)
	cLen := len(charset)
	for i := range buf {
		buf[i] = charset[g.rng.Intn(cLen)]
	}
	return string(buf)
}

// randomName returns a name with only English characters, length in [10, 15].
func (g *Generator) randomName() string {
	return g.randomString(letters, 10, 15)
}

// randomAddress returns an address with mixed alphanumeric + spaces,
// length in [15, 20].
//
// The schema example shows addresses like "12 abc dfsf LdUE", so we guarantee
// at least one digit and one space by inserting them at deterministic positions
// before randomising the rest.
func (g *Generator) randomAddress() string {
	length := 15 + g.rng.Intn(6) // [15, 20]
	buf := make([]byte, length)

	// Force a digit at position 0 (mirrors the schema example "12 abc …")
	buf[0] = byte('0' + g.rng.Intn(10))
	// Force a space at position 2 so the address looks like "NN …"
	buf[2] = ' '

	cLen := len(alphaNumSpace)
	for i := range buf {
		if i == 0 || i == 2 {
			continue // already set
		}
		buf[i] = alphaNumSpace[g.rng.Intn(cLen)]
	}
	return string(buf)
}

// randomContinent picks one of the six valid continent strings at random.
func (g *Generator) randomContinent() string {
	return continents[g.rng.Intn(len(continents))]
}

// GeneratePerson creates a fully populated random Person.
func (g *Generator) GeneratePerson() *Person {
	id := g.flag
	g.flag += 1
	return NewPerson(
		int32(id),
		g.randomName(),
		g.randomAddress(),
		g.randomContinent(),
	)
}

// GenerateToCSV writes `count` random Person records as CSV lines to `filePath`.
//
// Performance choices:
//   - bufio.Writer with a 4 MB buffer reduces the number of write syscalls by
//     ~4 orders of magnitude compared to unbuffered writes.
//   - strings.Builder is used to build each CSV line without repeated heap
//     allocations.
//   - We write the header once, then stream records directly — no in-memory
//     accumulation of the full dataset, keeping memory usage constant regardless
//     of `count`.
func GenerateToCSV(filePath string, count int) error {
	file, err := os.Create(filePath)
	//fmt.Println("function called....")
	if err != nil {
		return fmt.Errorf("create file %q: %w", filePath, err)
	}
	defer file.Close()

	// 4 MB write buffer — large enough to amortise syscall overhead without
	// blowing the 2 GB memory budget even slightly.
	const bufSize = 4 * 1024 * 1024
	w := bufio.NewWriterSize(file, bufSize)

	// CSV header
	if _, err := fmt.Fprintln(w, "id,name,address,continent"); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	g := NewGenerator()
	var sb strings.Builder

	start := time.Now()

	for i := 0; i < count; i++ {
		p := g.GeneratePerson()

		// Build CSV line into a reused Builder to cut allocations.
		sb.Reset()
		sb.WriteString(fmt.Sprintf("%d", p.ID))
		sb.WriteByte(',')
		sb.WriteString(p.Name)
		sb.WriteByte(',')
		sb.WriteString(p.Address)
		sb.WriteByte(',')
		sb.WriteString(p.Continent)
		sb.WriteByte('\n')

		if _, err := w.WriteString(sb.String()); err != nil {
			return fmt.Errorf("write record %d: %w", i, err)
		}

		// Progress report every 5 million records so the caller can see activity.
		if (i+1)%5_000_000 == 0 {
			fmt.Printf("  generated %d / %d records (%.1fs elapsed)\n",
				i+1, count, time.Since(start).Seconds())
		}
	}

	// Flush the remaining buffered data to disk.
	if err := w.Flush(); err != nil {
		return fmt.Errorf("flush writer: %w", err)
	}

	fmt.Printf("Done. %d records written to %q in %.2fs\n",
		count, filePath, time.Since(start).Seconds())
	return nil
}
