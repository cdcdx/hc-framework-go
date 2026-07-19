package cache

import (
	"sync"
	"testing"
	"time"
)

func TestHotKeyDetector_TopN(t *testing.T) {
	d := NewHotKeyDetector(3, 1, 2)
	for i := 0; i < 100; i++ {
		d.Record("hot")
	}
	d.Record("cold")

	if !d.IsHot("hot") {
		t.Fatal("expected 'hot' to be hot")
	}
	if d.IsHot("cold") {
		t.Fatal("expected 'cold' not to be hot")
	}
	ranks := d.GetHotKeys()
	if ranks["hot"] != 100 {
		t.Fatalf("expected hot rank 100, got %d", ranks["hot"])
	}
}

func TestHotKeyDetector_NoGlobalLockOnRecord(t *testing.T) {
	d := NewHotKeyDetector(3, 5, 1)
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				d.Record("k")
				_ = d.IsHot("k")
			}
		}()
	}
	wg.Wait()

	if !d.IsHot("k") {
		t.Fatal("expected 'k' to be hot after concurrent records")
	}
}

func TestHotKeyDetector_Rotation(t *testing.T) {
	d := NewHotKeyDetector(2, 10, 1)
	d.Record("a")
	time.Sleep(time.Second + 100*time.Millisecond)
	d.Record("a")
	if !d.IsHot("a") {
		t.Fatal("expected 'a' hot after rotation within window")
	}
}

func TestHotKeyDetector_ZeroTopN(t *testing.T) {
	// TopN=0：任何 key 都不应被视为热点
	d := NewHotKeyDetector(3, 0, 2)
	for i := 0; i < 100; i++ {
		d.Record("k")
	}
	if d.IsHot("k") {
		t.Fatal("expected 'k' not hot when TopN=0")
	}
	ranks := d.GetHotKeys()
	if len(ranks) != 0 {
		t.Fatalf("expected empty ranks for TopN=0, got %d entries", len(ranks))
	}
}

func TestHotKeyDetector_MultiplierZero(t *testing.T) {
	// multiplier=0（或 1）：HotTTL 应退回 baseTTL，不额外延长
	d := NewHotKeyDetector(3, 10, 0)
	base := 10 * time.Second
	hot := d.HotTTL(base)
	if hot != base {
		t.Fatalf("HotTTL with multiplier=0 should equal base, got %v", hot)
	}
}

func TestHotKeyDetector_EmptyWindow(t *testing.T) {
	// windowSeconds=0：应被钳制到 >=2 桶，检测器正常工作
	d := NewHotKeyDetector(0, 10, 2)
	d.Record("k")
	if !d.IsHot("k") {
		t.Fatal("expected 'k' hot with clamped window")
	}
}
