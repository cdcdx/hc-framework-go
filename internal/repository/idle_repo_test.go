package repository

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/cache"
)

// fakeL2 是 cache.L2Cache 的最小实现，仅 SScan 按 key 返回预设成员，其余方法返回零值。
// 用于验证 ScanActiveSessions 的分片合并逻辑。
type fakeL2 struct {
	scans map[string][]string
}

func (f *fakeL2) Get(ctx context.Context, key string) (interface{}, bool, error) {
	return nil, false, nil
}
func (f *fakeL2) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	return nil
}
func (f *fakeL2) Delete(ctx context.Context, keys ...string) error     { return nil }
func (f *fakeL2) Exists(ctx context.Context, key string) (bool, error) { return false, nil }
func (f *fakeL2) GetMulti(ctx context.Context, keys []string) (map[string]interface{}, error) {
	return nil, nil
}
func (f *fakeL2) SetMulti(ctx context.Context, items map[string]interface{}, ttl time.Duration) error {
	return nil
}
func (f *fakeL2) Pipeline(ctx context.Context) (cache.Pipeline, error) { return &fakePipeline{}, nil }
func (f *fakeL2) Lock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return true, nil
}
func (f *fakeL2) Unlock(ctx context.Context, key string) error { return nil }
func (f *fakeL2) RefreshLock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return true, nil
}
func (f *fakeL2) Publish(ctx context.Context, channel string, message interface{}) error {
	return nil
}
func (f *fakeL2) Subscribe(ctx context.Context, channel string, handler func(msg string)) error {
	return nil
}
func (f *fakeL2) ConfigSet(ctx context.Context, key, value string) error { return nil }
func (f *fakeL2) SAdd(ctx context.Context, key, member string) error     { return nil }
func (f *fakeL2) SRem(ctx context.Context, key, member string) error     { return nil }
func (f *fakeL2) IncrBy(ctx context.Context, key string, value int64) (int64, error) {
	return 0, nil
}
func (f *fakeL2) BatchExists(ctx context.Context, keys []string) ([]bool, error) {
	out := make([]bool, len(keys))
	return out, nil
}
func (f *fakeL2) SScan(ctx context.Context, key string) ([]string, error) {
	if f.scans != nil {
		if v, ok := f.scans[key]; ok {
			return v, nil
		}
	}
	return nil, nil
}

type fakePipeline struct{}

func (p *fakePipeline) Get(ctx context.Context, key string) error { return nil }
func (p *fakePipeline) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	return nil
}
func (p *fakePipeline) Delete(ctx context.Context, keys ...string) error { return nil }
func (p *fakePipeline) Exec(ctx context.Context) ([]interface{}, error)  { return nil, nil }

func TestParseHeartbeatKey(t *testing.T) {
	cases := []struct {
		name    string
		fullKey string
		wantU   string
		wantD   string
		wantOK  bool
	}{
		{
			name:    "含 keyPrefix 的正常完整 key",
			fullKey: "idle:active:idle:hb:user123:devA",
			wantU:   "user123",
			wantD:   "devA",
			wantOK:  true,
		},
		{
			name:    "deviceID 含冒号（按 idle:hb: 标记定位，device 取剩余全部）",
			fullKey: "idle:active:idle:hb:user123:phone:1",
			wantU:   "user123",
			wantD:   "phone:1",
			wantOK:  true,
		},
		{
			name:    "非心跳 key（无 idle:hb: 标记）",
			fullKey: "idle:active:idle:other:user123",
			wantOK:  false,
		},
		{
			name:    "空字符串",
			fullKey: "",
			wantOK:  false,
		},
		{
			name:    "只有 user 没有 device（rest 无法 SplitN 为两段）",
			fullKey: "idle:hb:user123",
			wantOK:  false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			u, d, ok := ParseHeartbeatKey(c.fullKey)
			if ok != c.wantOK {
				t.Fatalf("key=%q ok=%v want %v", c.fullKey, ok, c.wantOK)
			}
			if !c.wantOK {
				return
			}
			if u != c.wantU || d != c.wantD {
				t.Fatalf("key=%q got (%q,%q) want (%q,%q)", c.fullKey, u, d, c.wantU, c.wantD)
			}
		})
	}
}

func TestParseActiveMember(t *testing.T) {
	u, d := ParseActiveMember("userX" + activeMemberSep + "devY")
	if u != "userX" || d != "devY" {
		t.Fatalf("ParseActiveMember got (%q,%q) want (userX,devY)", u, d)
	}
	// 无分隔符 → 空
	_, _ = ParseActiveMember("nodevice")
}

func TestActiveSetShard(t *testing.T) {
	// 正常分片数：确定性 + 范围 + 分布
	r := &IdleRepository{activeSetShards: 256}
	const uid = "user-abc-123"
	s1 := r.activeSetShard(uid)
	s2 := r.activeSetShard(uid)
	if s1 != s2 {
		t.Fatalf("activeSetShard not deterministic: %d vs %d", s1, s2)
	}
	if s1 < 0 || s1 >= 256 {
		t.Fatalf("activeSetShard out of range: %d", s1)
	}

	// 分布：1000 个不同 userID 应落到多个分片
	seen := make(map[int]struct{})
	for i := 0; i < 1000; i++ {
		seen[r.activeSetShard(fmt.Sprintf("u-%d", i))] = struct{}{}
	}
	if len(seen) <= 1 {
		t.Fatalf("activeSetShard distribution too poor: only %d distinct shards", len(seen))
	}

	// <=0 退化：单集合（shard 0、count 1）
	r0 := &IdleRepository{activeSetShards: 0}
	if got := r0.activeSetShard("any"); got != 0 {
		t.Fatalf("activeSetShard(<=0) want 0 got %d", got)
	}
	if got := r0.activeSetShardCount(); got != 1 {
		t.Fatalf("activeSetShardCount(<=0) want 1 got %d", got)
	}
	key := r0.activeSetKeyOf("any")
	if key != fmt.Sprintf(activeSetKeyFmt, 0) {
		t.Fatalf("activeSetKeyOf(<=0) want %q got %q", fmt.Sprintf(activeSetKeyFmt, 0), key)
	}
}

func TestScanActiveSessions(t *testing.T) {
	fake := &fakeL2{scans: map[string][]string{
		fmt.Sprintf(activeSetKeyFmt, 0): {"u1" + activeMemberSep + "d1"},
		fmt.Sprintf(activeSetKeyFmt, 1): nil,
		fmt.Sprintf(activeSetKeyFmt, 2): {"u2" + activeMemberSep + "d2", "u3" + activeMemberSep + "d3"},
		fmt.Sprintf(activeSetKeyFmt, 3): nil,
	}}
	r := &IdleRepository{
		activeSetShards: 4,
		cacheMgr:        &cache.Manager{L2: fake},
	}

	got, err := r.ScanActiveSessions(context.Background())
	if err != nil {
		t.Fatalf("ScanActiveSessions err: %v", err)
	}
	want := []string{
		"u1" + activeMemberSep + "d1",
		"u2" + activeMemberSep + "d2",
		"u3" + activeMemberSep + "d3",
	}
	if len(got) != len(want) {
		t.Fatalf("ScanActiveSessions got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ScanActiveSessions[%d]=%q want %q", i, got[i], want[i])
		}
	}
}

// TestToInit64 验证缓存反序列化结果（float64/int64/string/int）安全转为 int64。
func TestToInit64(t *testing.T) {
	cases := []struct {
		in   interface{}
		want int64
		ok   bool
	}{
		{float64(123), 123, true},
		{int64(456), 456, true},
		{int(789), 789, true},
		{"1024", 1024, true},
		{"abc", 0, false},
		{nil, 0, false},
	}
	for _, c := range cases {
		got, ok := toInt64(c.in)
		if ok != c.ok || got != c.want {
			t.Fatalf("toInt64(%v)=(%d,%v) want (%d,%v)", c.in, got, ok, c.want, c.ok)
		}
	}
}

// TestEndOfLocalDay 验证每日计数器 TTL 为正的有限时长（< 25h）。
func TestEndOfLocalDay(t *testing.T) {
	d := endOfLocalDay()
	if d <= 0 || d > 25*time.Hour {
		t.Fatalf("endOfLocalDay=%v want (0,25h]", d)
	}
}

// aliveFakeL2 在 fakeL2 基础上可控地返回 BatchExists 结果（按 key 索引）。
type aliveFakeL2 struct {
	*fakeL2
	alive []bool
}

func (f *aliveFakeL2) BatchExists(ctx context.Context, keys []string) ([]bool, error) {
	out := make([]bool, len(keys))
	for i := range keys {
		if i < len(f.alive) {
			out[i] = f.alive[i]
		}
	}
	return out, nil
}

// TestBatchHeartbeatAlive 验证：仅心跳 Key 缺失的成员被判为离线（dead），
// 且无法解析的成员（空 key）不会进入 dead 集合被误清理。
func TestBatchHeartbeatAlive(t *testing.T) {
	members := []string{
		"u1" + activeMemberSep + "d1",
		"u2" + activeMemberSep + "d2",
		"u3" + activeMemberSep + "d3",
		"unparseable", // 无分隔符，解析失败
	}
	af := &aliveFakeL2{
		fakeL2: &fakeL2{},
		alive:  []bool{true, false, true, false}, // u1/u3 存活；u2 离线；unparseable 不计入 alive
	}
	r := &IdleRepository{activeSetShards: 1, cacheMgr: &cache.Manager{L2: af}}

	dead, err := r.BatchHeartbeatAlive(context.Background(), members)
	if err != nil {
		t.Fatalf("BatchHeartbeatAlive err: %v", err)
	}
	if len(dead) != 1 || dead[0] != "u2"+activeMemberSep+"d2" {
		t.Fatalf("BatchHeartbeatAlive dead=%v want [u2\x1fd2]", dead)
	}
}

// storeFakeL2 在 fakeL2 基础上实现可读写的 Get/Set/IncrBy，用于验证每日积分计数器。
type storeFakeL2 struct {
	*fakeL2
	mu     sync.Mutex
	getMap map[string]interface{}
}

func (f *storeFakeL2) Get(ctx context.Context, key string) (interface{}, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.getMap[key]
	return v, ok, nil
}
func (f *storeFakeL2) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getMap == nil {
		f.getMap = map[string]interface{}{}
	}
	f.getMap[key] = value
	return nil
}

// IncrBy 模拟 Redis INCRBY：基于当前缓存值累加并更新底层 key（测试用）。
func (f *storeFakeL2) IncrBy(ctx context.Context, key string, value int64) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getMap == nil {
		f.getMap = map[string]interface{}{}
	}
	cur := int64(0)
	if v, ok := f.getMap[key]; ok {
		if n, ok := toInt64(v); ok {
			cur = n
		}
	}
	cur += value
	f.getMap[key] = cur
	return cur, nil
}

// TestDailyPointsCounter 验证 GetDailyPoints 命中 Redis 缓存即返回（不再回源 DB），
// 且 IncrDailyPoints 累加正确。
func TestDailyPointsCounter(t *testing.T) {
	sf := &storeFakeL2{fakeL2: &fakeL2{}}
	r := &IdleRepository{activeSetShards: 1, cacheMgr: &cache.Manager{L2: sf}}

	// 预热：写入当日计数器
	key := r.dailyPointsKey("u1")
	if err := sf.Set(context.Background(), key, int64(100), time.Hour); err != nil {
		t.Fatalf("set daily counter: %v", err)
	}
	got, err := r.GetDailyPoints(context.Background(), "u1")
	if err != nil {
		t.Fatalf("GetDailyPoints err: %v", err)
	}
	if got != 100 {
		t.Fatalf("GetDailyPoints got %d want 100 (should read from Redis, no DB)", got)
	}

	// 累加后读取应反映最新值
	r.IncrDailyPoints(context.Background(), "u1", 50)
	got, err = r.GetDailyPoints(context.Background(), "u1")
	if err != nil {
		t.Fatalf("GetDailyPoints after incr err: %v", err)
	}
	if got != 150 {
		t.Fatalf("GetDailyPoints after incr got %d want 150", got)
	}
}
