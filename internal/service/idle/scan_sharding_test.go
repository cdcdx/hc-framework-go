package idle

import (
	"context"
	"testing"

	"github.com/cdcdx/hc-framework-go/internal/config"
)

// TestCalcPodSetShards 验证分片分配：每 Pod 负责互不重叠的集合分片，并集覆盖全部分片，且负载均衡。
func TestCalcPodSetShards(t *testing.T) {
	const setShards = 256
	const total = 4

	owned := make([][]int, total)
	union := make(map[int]bool)
	for p := 0; p < total; p++ {
		sh := calcPodSetShards(p, total, setShards)
		if len(sh) == 0 {
			t.Fatalf("pod %d owns no shards", p)
		}
		owned[p] = sh
		for _, s := range sh {
			if union[s] {
				t.Fatalf("set shard %d owned by multiple pods", s)
			}
			union[s] = true
		}
	}
	if len(union) != setShards {
		t.Fatalf("union of owned shards = %d, want %d", len(union), setShards)
	}

	// 单副本（total<=1）应返回全部分片
	all := calcPodSetShards(0, 1, setShards)
	if len(all) != setShards {
		t.Fatalf("single-pod should own all %d shards, got %d", setShards, len(all))
	}

	// setShards<=0 返回 nil（service 层据此退化为全量扫描）
	if calcPodSetShards(0, 1, 0) != nil {
		t.Fatal("zero setShards should return nil")
	}
}

// TestResolvePodShard 验证 Pod 序号/总数解析：配置优先，环境变量覆盖，且做边界钳制。
func TestResolvePodShard(t *testing.T) {
	t.Setenv("HC_POD_INDEX", "")
	t.Setenv("HC_POD_TOTAL", "")

	// 配置值直接生效
	idx, total := ResolvePodShard(config.ScanShardingConfig{PodIndex: 2, PodTotal: 5})
	if idx != 2 || total != 5 {
		t.Fatalf("config values: got %d/%d, want 2/5", idx, total)
	}

	// 环境变量覆盖（默认 0/1 时被 HC_POD_* 覆盖）
	t.Setenv("HC_POD_INDEX", "3")
	t.Setenv("HC_POD_TOTAL", "7")
	idx, total = ResolvePodShard(config.ScanShardingConfig{PodIndex: 0, PodTotal: 1})
	if idx != 3 || total != 7 {
		t.Fatalf("env override: got %d/%d, want 3/7", idx, total)
	}

	// 序号越界钳制到 [0, total-1]
	idx, total = ResolvePodShard(config.ScanShardingConfig{PodIndex: 99, PodTotal: 5})
	if idx != 4 {
		t.Fatalf("index clamp: got %d, want 4 (total-1)", idx)
	}
	if total != 5 {
		t.Fatalf("total should stay 5, got %d", total)
	}

	// total<=0 且无 env 覆盖时钳制为 1
	t.Setenv("HC_POD_TOTAL", "")
	_, total = ResolvePodShard(config.ScanShardingConfig{PodIndex: 0, PodTotal: 0})
	if total != 1 {
		t.Fatalf("total<=0 (no env) should clamp to 1, got %d", total)
	}
}

// TestSessionSetShard 验证 userID → 集合分片映射：范围正确、对相同输入稳定、对 total 取模分配一致。
func TestSessionSetShard(t *testing.T) {
	const setShards = 256
	// 范围检查：所有 userID 必须落在 [0, setShards)
	for _, u := range []string{"u1", "u-abc", "device_x", "10086", "中文用户"} {
		sh := sessionSetShard(u, setShards)
		if sh < 0 || sh >= setShards {
			t.Fatalf("sessionSetShard(%q)=%d out of range [0,%d)", u, sh, setShards)
		}
		// 相同输入必须稳定（事件驱动与 scanner 必须一致）
		if sessionSetShard(u, setShards) != sh {
			t.Fatalf("sessionSetShard not stable for %q", u)
		}
	}
	// setShards<=0 退化为 0
	if sessionSetShard("any", 0) != 0 {
		t.Fatal("setShards<=0 should map to shard 0")
	}
}

// TestOwnsSetShard 验证取模归属规则：i % total == index 才归属本 Pod。
func TestOwnsSetShard(t *testing.T) {
	const total = 4
	for idx := 0; idx < total; idx++ {
		for i := 0; i < 256; i++ {
			want := i%total == idx
			if ownsSetShard(i, idx, total) != want {
				t.Fatalf("ownsSetShard(%d,%d,%d)=%v, want %v", i, idx, total, !want, want)
			}
		}
	}
	// total<=1 时所有 Pod 拥有全部分片（向后兼容单/双实例）
	if !ownsSetShard(123, 0, 1) {
		t.Fatal("total<=1 should own all shards")
	}
}

// TestOwnsSessionByShard 验证 IdleService 按分片过滤会话：分片启用时仅归属本 Pod 的会话返回 true，
// 未启用（单实例）时所有会话返回 true。与 scanner 的 calcPodSetShards 取模规则保持一致。
func TestOwnsSessionByShard(t *testing.T) {
	const setShards, total = 256, 4
	for idx := 0; idx < total; idx++ {
		svc := &IdleService{scanShardingEnabled: true, scanPodIndex: idx, scanPodTotal: total, scanSetShards: setShards}
		for u := 0; u < 200; u++ {
			userID := "user-" + string(rune('a'+u%26)) + "-id" // 构造若干不同 userID
			got := svc.ownsSessionByShard(userID)
			want := sessionSetShard(userID, setShards)%total == idx
			if got != want {
				t.Fatalf("pod %d ownsSessionByShard(%q)=%v, want %v", idx, userID, got, want)
			}
		}
	}
	// 未启用分片：所有会话都归属本 Pod
	svcAll := &IdleService{scanShardingEnabled: false, scanPodIndex: 0, scanPodTotal: 1, scanSetShards: setShards}
	if !svcAll.ownsSessionByShard("any-user") {
		t.Fatal("sharding disabled should own all sessions")
	}
}

// TestComputeOwnedShards 验证死分片接管：Leader 接管失活 Pod 的全部集合分片；非 Leader 仅自身分片。
func TestComputeOwnedShards(t *testing.T) {
	const setShards, total = 256, 4

	// 全部存活：Leader 与非 Leader 都只扫自身分片（无死分片可接管）
	allAlive := map[int]bool{0: true, 1: true, 2: true, 3: true}
	own := calcPodSetShards(0, total, setShards)
	for _, leader := range []bool{false, true} {
		got := ComputeOwnedShards(0, total, setShards, allAlive, leader)
		if len(got) != len(own) {
			t.Fatalf("all-alive leader=%v: got %d shards, want %d", leader, len(got), len(own))
		}
	}

	// 仅 Pod0 存活，其余失活：
	//  - 非 Leader：仅自身分片
	//  - Leader：自身 + 1/2/3 的分片 = 全量 256
	alive0 := map[int]bool{0: true}
	nonLeader := ComputeOwnedShards(0, total, setShards, alive0, false)
	if len(nonLeader) != len(own) {
		t.Fatalf("non-leader dead takeover: got %d shards, want %d (own only)", len(nonLeader), len(own))
	}
	leaderAll := ComputeOwnedShards(0, total, setShards, alive0, true)
	if len(leaderAll) != setShards {
		t.Fatalf("leader dead takeover: got %d shards, want %d (full)", len(leaderAll), setShards)
	}
	// 全量集合应为 [0, setShards) 且无重复
	seen := make(map[int]bool, setShards)
	for _, s := range leaderAll {
		if seen[s] {
			t.Fatalf("duplicate set shard %d in takeover result", s)
		}
		seen[s] = true
	}
	if len(seen) != setShards {
		t.Fatalf("leader takeover union = %d shards, want %d", len(seen), setShards)
	}

	// total<=1：恒返回全量（向后兼容单/双实例）
	if got := ComputeOwnedShards(0, 1, setShards, map[int]bool{0: true}, true); len(got) != setShards {
		t.Fatalf("total<=1 should return all %d shards, got %d", setShards, len(got))
	}
}

// TestResolvePodShardHeadless 验证 pod_discovery=headless 时：
//   - pod_total 通过注入的 resolver（模拟 DNS A 记录数）动态获得；
//   - pod_index 在未设置 HC_POD_INDEX 时从 POD_NAME 提取 StatefulSet ordinal。
func TestResolvePodShardHeadless(t *testing.T) {
	t.Setenv("HC_POD_INDEX", "")
	t.Setenv("HC_POD_TOTAL", "")
	t.Setenv("POD_NAME", "hc-framework-2")

	fake := func(ctx context.Context, service string) (int, error) {
		if service == "" {
			t.Fatalf("unexpected empty headless service")
		}
		return 4, nil // 模拟 4 个就绪 Pod
	}
	idx, total := resolvePodShard(context.Background(),
		config.ScanShardingConfig{PodIndex: 0, PodTotal: 0, PodDiscovery: "headless", HeadlessService: "hcf"}, fake)
	if idx != 2 {
		t.Fatalf("pod index from POD_NAME ordinal: got %d, want 2", idx)
	}
	if total != 4 {
		t.Fatalf("pod total from headless DNS: got %d, want 4", total)
	}

	// headless 模式优先级：HC_POD_TOTAL 静态注入不应覆盖 DNS 动态发现（否则 HPA 扩缩失效）
	t.Setenv("HC_POD_TOTAL", "9")
	_, total = resolvePodShard(context.Background(),
		config.ScanShardingConfig{PodIndex: 0, PodTotal: 0, PodDiscovery: "headless", HeadlessService: "hcf"}, fake)
	if total != 4 {
		t.Fatalf("headless DNS should take precedence over HC_POD_TOTAL: got %d, want 4", total)
	}

	// env 模式（pod_discovery 非 headless）：HC_POD_TOTAL 生效
	t.Setenv("HC_POD_TOTAL", "9")
	_, total = resolvePodShard(context.Background(),
		config.ScanShardingConfig{PodIndex: 0, PodTotal: 0, PodDiscovery: "env", HeadlessService: "hcf"}, fake)
	if total != 9 {
		t.Fatalf("env mode should honor HC_POD_TOTAL: got %d, want 9", total)
	}
}

// TestPodOrdinal 验证从 POD_NAME / HOSTNAME 提取 StatefulSet ordinal。
func TestPodOrdinal(t *testing.T) {
	cases := []struct {
		env  string
		val  string
		want int
	}{
		{"POD_NAME", "hc-framework-3", 3},
		{"HOSTNAME", "idle-0", 0},
		{"POD_NAME", "no-digit", -1},
		{"POD_NAME", "", -1},
	}
	for _, c := range cases {
		t.Setenv("POD_NAME", "")
		t.Setenv("HOSTNAME", "")
		if c.val != "" {
			t.Setenv(c.env, c.val)
		}
		if got := podOrdinal(); got != c.want {
			t.Fatalf("podOrdinal(%s=%q)=%d, want %d", c.env, c.val, got, c.want)
		}
	}
}
