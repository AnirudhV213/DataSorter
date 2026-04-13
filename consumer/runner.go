package consumer

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"
)

// RunAllSorters runs the id, name, and continent sorters in parallel.
//
// Each sorter holds at most one chunk in RAM at a time (chunkSize = 500K records).
// Memory budget breakdown:
//
//	Kafka JVM (pinned):               512 MB
//	3 × chunk array (500K × 130B):    195 MB
//	3 × msgCh buffer (50K × 130B):     20 MB
//	3 × fetch buffers (6MB × 3 parts):  54 MB
//	Go runtime + misc:                 ~50 MB
//	                                  --------
//	Total peak:                        ~831 MB  ← within 1 GB app limit
//
// All three sorters consume the source topic independently via separate
// Sarama client+consumer instances, and each writes to its own output topic,
// so there is no read or write contention between them.
//
// The errgroup cancels the shared context on the first error, causing all
// in-flight sorters to stop cleanly via ctx.Done().
func RunAllSorters(ctx context.Context, brokers []string) error {
	keys := []SortKey{SortByID, SortByName, SortByContinent}

	eg, ctx := errgroup.WithContext(ctx)

	for _, key := range keys {
		key := key
		eg.Go(func() error {
			fmt.Printf("\n═══ Sorter: %s ═══\n", key)
			s, err := NewSorter(brokers, key)
			if err != nil {
				return fmt.Errorf("build sorter %q: %w", key, err)
			}
			return s.Run(ctx)
		})
	}

	return eg.Wait()
}
