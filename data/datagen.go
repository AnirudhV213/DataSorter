package data

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"
)

// Schema: id (int32) | name (string) | address (string) | continent (string)

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

// alphaNumSpace contains characters used for address generation.
const alphaNumSpace = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "

// Person represents a single record matching the pipeline schema.
type Person struct {
	ID        int32
	Name      string
	Address   string
	Continent string
}

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

func NewGenerator() *Generator {
	return &Generator{
		flag: 1,
		rng:  rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// randomString builds a random string of length in [minLen, maxLen] drawn from charset.
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

// randomAddress returns a mixed alphanumeric+space address, length in [15, 20].
// A digit is forced at index 0 and a space at index 2 to match the schema example.
func (g *Generator) randomAddress() string {
	length := 15 + g.rng.Intn(6) // [15, 20]
	buf := make([]byte, length)

	buf[0] = byte('0' + g.rng.Intn(10))
	buf[2] = ' '

	cLen := len(alphaNumSpace)
	for i := range buf {
		if i == 0 || i == 2 {
			continue
		}
		buf[i] = alphaNumSpace[g.rng.Intn(cLen)]
	}
	return string(buf)
}

func (g *Generator) randomContinent() string {
	return continents[g.rng.Intn(len(continents))]
}

func (g *Generator) randomID() int32 {
	return int32(g.rng.Intn(50_000_000))
}

// GeneratePerson creates a fully populated random Person.
func (g *Generator) GeneratePerson() *Person {
	return NewPerson(
		g.randomID(),
		g.randomName(),
		g.randomAddress(),
		g.randomContinent(),
	)
}

// GenerateToCSV writes count random Person records as CSV lines to filePath.
// Uses a 4 MB write buffer and a reused strings.Builder to minimise allocations.
func GenerateToCSV(filePath string, count int) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("create file %q: %w", filePath, err)
	}
	defer file.Close()

	w := bufio.NewWriterSize(file, 4*1024*1024)

	if _, err := fmt.Fprintln(w, "id,name,address,continent"); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	g := NewGenerator()
	var sb strings.Builder

	start := time.Now()

	for i := 0; i < count; i++ {
		p := g.GeneratePerson()

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

		if (i+1)%5_000_000 == 0 {
			fmt.Printf("  generated %d / %d records (%.1fs elapsed)\n",
				i+1, count, time.Since(start).Seconds())
		}
	}

	if err := w.Flush(); err != nil {
		return fmt.Errorf("flush writer: %w", err)
	}

	fmt.Printf("Done. %d records written to %q in %.2fs\n",
		count, filePath, time.Since(start).Seconds())
	return nil
}
