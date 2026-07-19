package idle

import (
	"context"
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/config"
)

// scanMockL2 在 mockL2 基础上覆盖 SScan / Exists，用于驱动离线检测 scanner：
//   - scanMembers：ScanActiveSessions 遍历活跃集合返回的成员（userID<0x1f>deviceID）；
//   - alive：IsHeartbeatAlive 的返回值（true=在线，false=离线/超时）。
//
// 其余方法（Set/Delete/SRem/Subscribe/...）沿用 mockL2 的默认实现与调用记录。
type scanMockL2 struct {
	*mockL2
	scanMembers []string
	alive       bool
}

func (m *scanMockL2) SScan(ctx context.Context, key string) ([]string, error) {
	return m.scanMembers, nil
}

func (m *scanMockL2) Exists(ctx context.Context, key string) (bool, error) {
	return m.alive, nil
}

// BatchExists 覆盖 L2Cache 接口：离线检测 scanner 用其批量判活，返回值沿用 m.alive。
func (m *scanMockL2) BatchExists(ctx context.Context, keys []string) ([]bool, error) {
	out := make([]bool, len(keys))
	for i := range keys {
		out[i] = m.alive
	}
	return out, nil
}

// ────────── 离线检测 scanner（ScanAndSettleTimeout）──────────

func TestScanAndSettleTimeout(t *testing.T) {
	// 单分片：ScanActiveSessions 仅遍历一个集合，便于断言扫描结果（无重复成员）。
	mkCfg := func() *config.Config {
		c := baseIdleCfg()
		c.Idle.ActiveSetShards = 1
		return c
	}

	// Redis 心跳判活路径：扫描到心跳缺失（离线）的会话 → 自动结算为 timeout 并清理。
	t.Run("redis_offline_session_settled", func(t *testing.T) {
		cfg := mkCfg()
		member := "uScan" + "\x1f" + "dScan"
		l2 := &scanMockL2{mockL2: &mockL2{}, scanMembers: []string{member}, alive: false}
		svc := buildEventDrivenServiceWithDB(t, cfg, l2)
		// 会话启动 1 小时前（远超 2*heartbeat_interval 宽限）→ 不触发宽限重触
		insertActiveRecord(t, svc, "uScan", "dScan", time.Now().Add(-1*time.Hour))

		n, err := svc.ScanAndSettleTimeout(context.Background())
		if err != nil {
			t.Fatalf("ScanAndSettleTimeout err: %v", err)
		}
		if n != 1 {
			t.Fatalf("expected 1 settled, got %d", n)
		}
		rec := queryIdleRecord(t, svc, "uScan", "dScan")
		if rec.Status != "timeout" {
			t.Fatalf("offline session should settle as timeout, got %q", rec.Status)
		}
		if rec.PointsEarned <= 0 {
			t.Fatalf("settled session should earn points, got %d", rec.PointsEarned)
		}
		if len(l2.deletes) == 0 {
			t.Fatal("should delete heartbeat key after settle (avoid dangling)")
		}
		if len(l2.srems) == 0 {
			t.Fatal("should remove member from active set after settle (stop scanning)")
		}
	})

	// Redis 心跳判活路径：扫描到心跳仍在（在线）的会话 → 跳过结算，保持 active。
	t.Run("redis_alive_session_skipped", func(t *testing.T) {
		cfg := mkCfg()
		member := "uAlive" + "\x1f" + "dAlive"
		l2 := &scanMockL2{mockL2: &mockL2{}, scanMembers: []string{member}, alive: true}
		svc := buildEventDrivenServiceWithDB(t, cfg, l2)
		insertActiveRecord(t, svc, "uAlive", "dAlive", time.Now().Add(-1*time.Hour))

		n, err := svc.ScanAndSettleTimeout(context.Background())
		if err != nil {
			t.Fatalf("ScanAndSettleTimeout err: %v", err)
		}
		if n != 0 {
			t.Fatalf("alive session must not be settled, got %d", n)
		}
		rec := queryIdleRecord(t, svc, "uAlive", "dAlive")
		if rec.Status != "active" {
			t.Fatalf("alive session should remain active, got %q", rec.Status)
		}
	})
}

// TestScanTimeoutByDBFallback 验证 Redis 不可用时的降级 scanner（scanTimeoutByDB）：
// 按 DB last_heartbeat_at 范围扫描超时会话并结算。直接调用私有方法，绕过 HeartbeatRedisEnabled 分支选择，
// 真实覆盖 FindStaleActive + settleSession 协作（与生产 Redis 路径共用同一幂等结算逻辑）。
func TestScanTimeoutByDBFallback(t *testing.T) {
	cfg := baseIdleCfg()
	l2 := &mockL2{}
	svc := buildEventDrivenServiceWithDB(t, cfg, l2)

	// last_heartbeat_at 1 小时前 < now - timeoutThreshold(5min) → FindStaleActive 命中
	start := time.Now().Add(-1 * time.Hour)
	insertActiveRecord(t, svc, "uDB", "dDB", start)

	n, err := svc.scanTimeoutByDB(context.Background())
	if err != nil {
		t.Fatalf("scanTimeoutByDB err: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 settled via DB fallback, got %d", n)
	}
	rec := queryIdleRecord(t, svc, "uDB", "dDB")
	if rec.Status != "timeout" {
		t.Fatalf("DB fallback should settle as timeout, got %q", rec.Status)
	}
	if rec.PointsEarned <= 0 {
		t.Fatalf("settled session should earn points, got %d", rec.PointsEarned)
	}
}

// ────────── 主动结算（Stop / StopDevice）──────────

// TestIdleStop 验证 Stop：停止某用户全部活跃会话，标记 completed 并清理 Redis 侧状态；
// 无活跃会话时返回 ErrNotIdle（幂等）。
func TestIdleStop(t *testing.T) {
	cfg := baseIdleCfg()
	l2 := &mockL2{}
	svc := buildEventDrivenServiceWithDB(t, cfg, l2)
	insertActiveRecord(t, svc, "uStop", "d1", time.Now())

	rec, err := svc.Stop(context.Background(), "uStop")
	if err != nil {
		t.Fatalf("Stop err: %v", err)
	}
	if rec == nil || rec.Status != "completed" {
		t.Fatalf("expected completed, got %+v", rec)
	}
	got := queryIdleRecord(t, svc, "uStop", "d1")
	if got.Status != "completed" {
		t.Fatalf("expected completed in DB, got %q", got.Status)
	}
	if got.PointsEarned <= 0 {
		t.Fatalf("expected points earned on stop, got %d", got.PointsEarned)
	}
	if len(l2.deletes) == 0 {
		t.Fatal("should delete heartbeat key on stop")
	}
	if len(l2.srems) == 0 {
		t.Fatal("should remove active set member on stop")
	}

	// 再次停止：无活跃会话 → ErrNotIdle（主动结算幂等，不重复入账）
	if _, err := svc.Stop(context.Background(), "uStop"); err != ErrNotIdle {
		t.Fatalf("expected ErrNotIdle on second Stop, got %v", err)
	}
}

// TestIdleStopDevice 验证 StopDevice：停止指定设备；设备不存在 / 已结算 → ErrNotIdle。
func TestIdleStopDevice(t *testing.T) {
	cfg := baseIdleCfg()
	l2 := &mockL2{}
	svc := buildEventDrivenServiceWithDB(t, cfg, l2)
	insertActiveRecord(t, svc, "uSD", "dA", time.Now())

	rec, err := svc.StopDevice(context.Background(), "uSD", "dA")
	if err != nil {
		t.Fatalf("StopDevice err: %v", err)
	}
	if rec == nil || rec.Status != "completed" {
		t.Fatalf("expected completed, got %+v", rec)
	}
	got := queryIdleRecord(t, svc, "uSD", "dA")
	if got.Status != "completed" {
		t.Fatalf("expected completed in DB, got %q", got.Status)
	}

	// 同一设备已结算 → ErrNotIdle
	if _, err := svc.StopDevice(context.Background(), "uSD", "dA"); err != ErrNotIdle {
		t.Fatalf("expected ErrNotIdle for already-stopped device, got %v", err)
	}
	// 不同设备（不存在） → ErrNotIdle
	if _, err := svc.StopDevice(context.Background(), "uSD", "dOther"); err != ErrNotIdle {
		t.Fatalf("expected ErrNotIdle for unknown device, got %v", err)
	}
}
