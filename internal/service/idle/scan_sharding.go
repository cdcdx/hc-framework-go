package idle

import (
	"context"
	"fmt"
	"hash/fnv"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/cdcdx/hc-framework-go/internal/config"
)

// sessionSetShard 计算某 userID 落入的活跃集合分片（与 repository.activeSetShard 同算法：
// fnv32a(userID) % setShards）。setShards<=0 时返回 0（单集合）。
// 事件驱动结算 consumer 用它判断某过期心跳 key 是否归属本 Pod 分片，与离线检测 scanner 共用同一分片规则。
func sessionSetShard(userID string, setShards int) int {
	if setShards <= 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(userID))
	return int(h.Sum32() % uint32(setShards))
}

// ownsSetShard 判断 setShard 是否归属 (podIndex, podTotal) 的取模分配（i % total == index）。
// total<=1（未分片）时所有 Pod 都拥有全部分片，返回 true（向后兼容单/双实例）。
func ownsSetShard(setShard, podIndex, podTotal int) bool {
	if podTotal <= 1 {
		return true
	}
	return setShard%podTotal == podIndex
}

// ComputeOwnedShards 返回本 Pod 本次应扫描的集合分片全集：自身分片 + （若为 Leader 时）所有失活 Pod 的分片。
// 用于“死分片接管”：某 Pod 宕机后其存活 Key（idle:pod-alive:{i}）过期，Leader 将该 Pod 的集合分片
// 并入自身扫描范围，消除静态分片下“Pod 宕机 → 其分片永不扫描”的可用性缺口。
// alive 为当前存活的 Pod 序号集合（含本 Pod）；isLeader 表示本 Pod 是否为选主 Leader。
func ComputeOwnedShards(myIdx, total, setShards int, alive map[int]bool, isLeader bool) []int {
	owned := calcPodSetShards(myIdx, total, setShards)
	if isLeader {
		for d := 0; d < total; d++ {
			if !alive[d] {
				owned = append(owned, calcPodSetShards(d, total, setShards)...)
			}
		}
	}
	return owned
}

// podTotalResolver 解析 Headless Service 的当前就绪副本数（返回 DNS A 记录数量）。
// 抽离为函数类型以便测试注入（测试可用 fake resolver 模拟 DNS）。
type podTotalResolver func(ctx context.Context, service string) (int, error)

// defaultPodTotalResolver 通过 Go 标准库 DNS 解析 Headless Service FQDN 的 A 记录，
// 记录数即就绪 Pod 数（k8s 中 Headless Service 为每 Pod 生成一条 A 记录）。
func defaultPodTotalResolver(ctx context.Context, service string) (int, error) {
	ips, err := net.DefaultResolver.LookupHost(ctx, service)
	if err != nil {
		return 0, err
	}
	return len(ips), nil
}

// ResolvePodShard 计算本 Pod 在分片扫描中的序号与总分片数（导出供 main 故障转移协调器复用）。
// 优先级：配置值 > Headless Service 动态发现（pod_discovery=headless）> 环境变量（HC_POD_INDEX / HC_POD_TOTAL）> 1。
// 注意 headless 模式下 DNS 发现的 pod_total 优先于 HC_POD_TOTAL 静态注入（见 resolvePodTotal），以保证 HPA 扩缩生效。
// 返回值保证：total >= 1，0 <= index < total。
func ResolvePodShard(cfg config.ScanShardingConfig) (int, int) {
	return resolvePodShard(context.Background(), cfg, defaultPodTotalResolver)
}

// resolvePodShard 内部实现，resolver 可注入（测试用）。
func resolvePodShard(ctx context.Context, cfg config.ScanShardingConfig, resolver podTotalResolver) (int, int) {
	index := resolvePodIndex(cfg)
	total := resolvePodTotal(ctx, cfg, resolver)
	if total <= 0 {
		total = 1
	}
	if index < 0 {
		index = 0
	}
	if index >= total {
		index = total - 1
	}
	return index, total
}

// resolvePodIndex 解析本 Pod 分片序号：配置值 > HC_POD_INDEX > POD_NAME/HOSTNAME 中的 StatefulSet ordinal。
func resolvePodIndex(cfg config.ScanShardingConfig) int {
	if cfg.PodIndex > 0 {
		return cfg.PodIndex
	}
	if v := os.Getenv("HC_POD_INDEX"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	if n := podOrdinal(); n >= 0 {
		return n
	}
	return 0
}

// resolvePodTotal 解析分片总数：配置值 > Headless Service DNS（pod_discovery=headless）> HC_POD_TOTAL 环境变量 > 1。
// 关键：headless 模式下 DNS 动态发现必须优先于 HC_POD_TOTAL 静态注入，否则 StatefulSet 注入的 HC_POD_TOTAL
// 会覆盖 HPA 扩缩后的真实副本数，使“动态发现”形同虚设。HC_POD_TOTAL 仅在非 headless（env 模式）或 DNS 失败时作为回退。
// 均无结果时回退为 1（全量扫描，向后兼容）。
func resolvePodTotal(ctx context.Context, cfg config.ScanShardingConfig, resolver podTotalResolver) int {
	if cfg.PodTotal > 1 {
		return cfg.PodTotal
	}
	if cfg.PodDiscovery == "headless" && cfg.HeadlessService != "" {
		if n, err := resolver(ctx, headlessFQDN(cfg.HeadlessService)); err == nil && n > 0 {
			return n
		}
	}
	if v := os.Getenv("HC_POD_TOTAL"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 1
}

// podOrdinal 从 POD_NAME / HOSTNAME 提取 StatefulSet ordinal（形如 hc-framework-2 → 2）。
// 未设置或无法解析时返回 -1（调用方回退为 0）。
func podOrdinal() int {
	for _, env := range []string{"POD_NAME", "HOSTNAME"} {
		v := os.Getenv(env)
		if v == "" {
			continue
		}
		if i := strings.LastIndex(v, "-"); i >= 0 && i+1 < len(v) {
			if n, err := strconv.Atoi(v[i+1:]); err == nil {
				return n
			}
		}
	}
	return -1
}

// headlessFQDN 构造 Headless Service 的集群内 FQDN：<name>.<namespace>.svc.cluster.local。
// namespace 取环境变量 POD_NAMESPACE（k8s 注入），缺省 "default"。
func headlessFQDN(name string) string {
	ns := os.Getenv("POD_NAMESPACE")
	if ns == "" {
		ns = "default"
	}
	return fmt.Sprintf("%s.%s.svc.cluster.local", name, ns)
}

// calcPodSetShards 返回本 Pod 应负责的活跃集合分片序号（基于 setShards 个集合分片）。
// 分配策略：i % total == index（取模分配，负载均衡且对集合数量非整除也安全）。
// total <= 1（未分片）时返回全部分片序号 [0, setShards)。
func calcPodSetShards(index, total, setShards int) []int {
	if setShards <= 0 {
		return nil
	}
	if total <= 1 {
		out := make([]int, setShards)
		for i := 0; i < setShards; i++ {
			out[i] = i
		}
		return out
	}
	var out []int
	for i := 0; i < setShards; i++ {
		if i%total == index {
			out = append(out, i)
		}
	}
	return out
}
