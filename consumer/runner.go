package consumer

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"
)

// RunAllSorters runs the three sorters — id, name, continent — in parallel.
//
// Memory safety analysis (why parallel doesn't OOM now):
//
//	Each sorter holds at most one chunk in RAM at a time (chunkSize = 500K records).
//	All buffers are sized conservatively via newParallelClientConfig():
//
//	  Kafka JVM (pinned):               512 MB
//	  3 × chunk array (500K × 130B):    195 MB
//	  3 × msgCh buffer (50K × 130B):     20 MB
//	  3 × fetch buffers (6MB × 3 parts):  54 MB
//	  Go runtime + misc:                 ~50 MB
//	                                    --------
//	  Total peak:                        ~831 MB  ← within 1 GB app limit
//
// All three sorters read the same source topic independently via separate
// Sarama client+consumer instances, so there is no coordination at the
// Kafka read layer. Each sorter writes to its own output topic (id / name /
// continent) so there is no write contention on the broker either.
//
// The errgroup propagates the first error and cancels the shared context,
// causing all in-flight sorters to stop cleanly via ctx.Done().
func RunAllSorters(ctx context.Context, brokers []string) error {
	keys := []SortKey{SortByID, SortByName, SortByContinent}

	wall := time.Now()

	eg, ctx := errgroup.WithContext(ctx)

	for _, key := range keys {
		key := key // capture loop variable
		eg.Go(func() error {
			fmt.Printf("\n═══ Sorter: %s ═══\n", key)
			s, err := NewSorter(brokers, key)
			if err != nil {
				return fmt.Errorf("build sorter %q: %w", key, err)
			}
			return s.Run(ctx)
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	fmt.Printf("\n✓ All three sorters complete. Total wall-clock: %.2fs\n",
		time.Since(wall).Seconds())
	return nil
}
