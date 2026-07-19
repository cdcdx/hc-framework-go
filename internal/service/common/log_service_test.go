package common

import (
	"sync"
	"testing"
)

func TestNextID_Monotonic(t *testing.T) {
	// 单 goroutine：返回严格递增的 ID
	last := nextID()
	for i := 0; i < 100; i++ {
		n := nextID()
		if n <= last {
			t.Fatalf("nextID() = %d, must be > %d", n, last)
		}
		last = n
	}
}

func TestNextID_ConcurrentUniqueness(t *testing.T) {
	const workers = 16
	const perWorker = 1000
	ids := make(chan int64, workers*perWorker)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				ids <- nextID()
			}
		}()
	}
	wg.Wait()
	close(ids)

	seen := make(map[int64]bool, workers*perWorker)
	for id := range ids {
		if seen[id] {
			t.Fatalf("duplicate id %d", id)
		}
		seen[id] = true
	}
	if len(seen) != workers*perWorker {
		t.Fatalf("expected %d unique ids, got %d", workers*perWorker, len(seen))
	}
}
