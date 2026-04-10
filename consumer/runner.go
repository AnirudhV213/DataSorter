package consumer

import (
	"context"
	"fmt"
	"time"
)

// RunAllSorters runs the three sorters — id, name, continent — sequentially.
//
// Why sequential and not parallel?
//
//	Each sorter loads the full 50 M record dataset into memory.
//	50 M records × ~120 bytes per Person struct ≈ 600 MB per sorter.
//	Running all three in parallel would require ~1.8 GB for records alone,
//	leaving no headroom for Kafka JVM (512 MB) and the OS.
//	Sequential execution peaks at ~600 MB, well within the 2 GB budget.
//
//	If more memory were available (bonus: "more machines"), the three sorters
//	could run in parallel across separate machines — each consuming the same
//	source topic via independent consumer groups.
func RunAllSorters(ctx context.Context, brokers []string) error {
	keys := []SortKey{SortByID, SortByName, SortByContinent}

	wall := time.Now()

	for _, key := range keys {
		fmt.Printf("\n═══ Sorter: %s ═══\n", key)

		s, err := NewSorter(brokers, key)
		if err != nil {
			return fmt.Errorf("build sorter %q: %w", key, err)
		}

		if err := s.Run(ctx); err != nil {
			return fmt.Errorf("sorter %q failed: %w", key, err)
		}
	}

	fmt.Printf("\n✓ All three sorters complete. Total wall-clock: %.2fs\n",
		time.Since(wall).Seconds())
	return nil
}
