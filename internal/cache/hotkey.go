package cache

import (
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// hotCounter 单个 key 在某一秒桶内的访问计数（原子累加，无需加锁）
type hotCounter struct {
	count int64
}

// hotSnapshot 缓存的热点结果快照，供 IsHot / GetHotKeys O(1) 查询
type hotSnapshot struct {
	set   map[string]struct{}
	ranks map[string]int64
}

// atomicBucket 一个「秒桶」：计数表与当前秒标记均原子访问，避免与 Record 竞争
type atomicBucket struct {
	m   atomic.Pointer[sync.Map]
	sec atomic.Int64
}

// HotKeyDetector 热点 Key 检测器（按秒分桶的滑动窗口 + 节流重算 TopN）。
//
// 设计目标（针对高并发读路径）：
//   - Record 在热路径上被每次缓存访问调用，必须无全局锁：仅对所属秒桶的计数器做原子累加；
//   - IsHot / GetHotKeys 在热路径上被多次调用，必须 O(1)：缓存 TopN 结果集，仅在节流间隔（默认 200ms）后按需重算；
//   - 内存有界：每个秒桶在轮转时整体替换为新 sync.Map，旧桶被 GC 回收，不会无限增长。
//
// 旧实现每次 Record 抢全局 mutex、每次 IsHot 全量扫描整个环形缓冲（最多 windowSeconds*10000 条）
// 并在同一把锁下做选择排序——在并发热路径上代价不可接受，本实现彻底消除该瓶颈。
type HotKeyDetector struct {
	bucketCount int
	buckets     []*atomicBucket
	windowSize  time.Duration
	topN        int
	multiplier  int

	computeInterval time.Duration
	computeMu       sync.Mutex
	hotSet          atomic.Pointer[hotSnapshot]
	lastCompute     atomic.Int64 // unix nanos
}

// NewHotKeyDetector 创建热点 Key 检测器
func NewHotKeyDetector(windowSeconds, topN, ttlMultiplier int) *HotKeyDetector {
	window := time.Duration(windowSeconds) * time.Second
	bucketCount := windowSeconds
	if bucketCount < 2 {
		bucketCount = 2
	}
	buckets := make([]*atomicBucket, bucketCount)
	for i := range buckets {
		b := &atomicBucket{}
		b.m.Store(&sync.Map{})
		b.sec.Store(-1)
		buckets[i] = b
	}
	return &HotKeyDetector{
		bucketCount:     bucketCount,
		buckets:         buckets,
		windowSize:      window,
		topN:            topN,
		multiplier:      ttlMultiplier,
		computeInterval: 200 * time.Millisecond,
	}
}

// Record 记录一次 Key 访问（无锁热路径）
func (d *HotKeyDetector) Record(key string) {
	sec := time.Now().Unix()
	b := d.bucketFor(sec)
	v, _ := b.m.Load().LoadOrStore(key, &hotCounter{})
	atomic.AddInt64(&v.(*hotCounter).count, 1)
}

// bucketFor 返回当前秒对应的桶，必要时轮转（替换为新表，旧表被 GC 回收）
func (d *HotKeyDetector) bucketFor(sec int64) *atomicBucket {
	b := d.buckets[int(sec)%d.bucketCount]
	if b.sec.Load() != sec {
		// 进入新的一秒：用 CAS 保证仅一个 goroutine 轮转该桶，避免重复重置
		if b.sec.CompareAndSwap(b.sec.Load(), sec) {
			b.m.Store(&sync.Map{})
		}
	}
	return b
}

// maybeCompute 节流地重算 TopN（双重检查，避免并发重复计算）
func (d *HotKeyDetector) maybeCompute() {
	now := time.Now().UnixNano()
	if now-d.lastCompute.Load() <= d.computeInterval.Nanoseconds() && d.hotSet.Load() != nil {
		return
	}
	d.computeMu.Lock()
	defer d.computeMu.Unlock()
	if time.Now().UnixNano()-d.lastCompute.Load() <= d.computeInterval.Nanoseconds() && d.hotSet.Load() != nil {
		return
	}
	d.computeHot()
}

// computeHot 汇总窗口内各桶计数，挑选 TopN，原子替换快照
func (d *HotKeyDetector) computeHot() {
	counter := make(map[string]int64)
	nowSec := time.Now().Unix()
	cutoff := nowSec - int64(d.windowSize/time.Second) - 1
	for i := 0; i < d.bucketCount; i++ {
		b := d.buckets[i]
		sec := b.sec.Load()
		if sec < 0 || sec < cutoff {
			continue
		}
		m := b.m.Load()
		m.Range(func(k, val interface{}) bool {
			key := k.(string)
			c := atomic.LoadInt64(&val.(*hotCounter).count)
			counter[key] += c
			return true
		})
	}

	// 取 TopN
	type kv struct {
		key   string
		count int64
	}
	all := make([]kv, 0, len(counter))
	for k, v := range counter {
		all = append(all, kv{k, v})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].count > all[j].count })
	if len(all) > d.topN {
		all = all[:d.topN]
	}

	set := make(map[string]struct{}, len(all))
	ranks := make(map[string]int64, len(all))
	for _, e := range all {
		set[e.key] = struct{}{}
		ranks[e.key] = e.count
	}
	d.hotSet.Store(&hotSnapshot{set: set, ranks: ranks})
	d.lastCompute.Store(time.Now().UnixNano())
}

// GetHotKeys 返回当前窗口内的 TopN 热点 Key 及其访问次数（节流重算）
func (d *HotKeyDetector) GetHotKeys() map[string]int64 {
	d.maybeCompute()
	snap := d.hotSet.Load()
	if snap == nil {
		return map[string]int64{}
	}
	return snap.ranks
}

// IsHot 判断 Key 是否热点（O(1) 查缓存快照，节流重算）
func (d *HotKeyDetector) IsHot(key string) bool {
	d.maybeCompute()
	snap := d.hotSet.Load()
	if snap == nil {
		return false
	}
	_, ok := snap.set[key]
	return ok
}

// HotTTL 计算热点 Key 的 TTL（基础TTL × multiplier × 随机偏移）
func (d *HotKeyDetector) HotTTL(baseTTL time.Duration) time.Duration {
	if d.multiplier <= 1 {
		return baseTTL
	}
	multiplied := baseTTL * time.Duration(d.multiplier)
	// 叠加 10% 随机偏移防雪崩
	jitter := time.Duration(float64(multiplied) * 0.1 * (rand.Float64()*2 - 1))
	return multiplied + jitter
}
