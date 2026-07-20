package idle

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/cache"
	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/db"
	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/cdcdx/hc-framework-go/internal/repository"
	"github.com/cdcdx/hc-framework-go/internal/service/common"

	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// mockL2 是 cache.L2Cache 的测试替身，重点记录 ConfigSet / Subscribe / Set / Delete / SRem 的调用
// （用于验证 StartEventDriven 的前置决策，以及 handleHBExpired 各分支对 Redis 的副作用）。
type mockL2 struct {
	configSetCalled  bool
	configSetKey     string
	configSetValue   string
	subscribeCalled  bool
	subscribeChannel string
	subDone          chan struct{} // 订阅协程完成信号（避免 goroutine 时序竞态）
	sets             []string
	deletes          []string
	srems            []string
}

func (m *mockL2) Get(ctx context.Context, key string) (interface{}, bool, error) {
	return nil, false, nil
}
func (m *mockL2) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	m.sets = append(m.sets, key)
	return nil
}
func (m *mockL2) Delete(ctx context.Context, keys ...string) error {
	m.deletes = append(m.deletes, keys...)
	return nil
}
func (m *mockL2) Exists(ctx context.Context, key string) (bool, error) { return false, nil }
func (m *mockL2) GetMulti(ctx context.Context, keys []string) (map[string]interface{}, error) {
	return nil, nil
}
func (m *mockL2) SetMulti(ctx context.Context, items map[string]interface{}, ttl time.Duration) error {
	return nil
}
func (m *mockL2) Pipeline(ctx context.Context) (cache.Pipeline, error) { return &mockPipeline{}, nil }
func (m *mockL2) Lock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return true, nil
}
func (m *mockL2) Unlock(ctx context.Context, key string) error { return nil }
func (m *mockL2) RefreshLock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return true, nil
}
func (m *mockL2) Publish(ctx context.Context, channel string, message interface{}) error {
	return nil
}
func (m *mockL2) Subscribe(ctx context.Context, channel string, handler func(msg string)) error {
	m.subscribeCalled = true
	m.subscribeChannel = channel
	if m.subDone != nil {
		close(m.subDone)
	}
	return nil
}
func (m *mockL2) ConfigSet(ctx context.Context, key, value string) error {
	m.configSetCalled = true
	m.configSetKey = key
	m.configSetValue = value
	return nil
}
func (m *mockL2) SAdd(ctx context.Context, key, member string) error { return nil }
func (m *mockL2) SRem(ctx context.Context, key, member string) error {
	m.srems = append(m.srems, key+":"+member)
	return nil
}
func (m *mockL2) IncrBy(ctx context.Context, key string, value int64) (int64, error) {
	return 0, nil
}
func (m *mockL2) BatchExists(ctx context.Context, keys []string) ([]bool, error) {
	out := make([]bool, len(keys))
	return out, nil
}
func (m *mockL2) SScan(ctx context.Context, key string) ([]string, error) {
	return nil, nil
}

type mockPipeline struct{}

func (p *mockPipeline) Get(ctx context.Context, key string) error { return nil }
func (p *mockPipeline) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	return nil
}
func (p *mockPipeline) Delete(ctx context.Context, keys ...string) error { return nil }
func (p *mockPipeline) Exec(ctx context.Context) ([]interface{}, error)  { return nil, nil }

// ────────── 集成测试用的 noop 依赖（避免触达真实 ES/ClickHouse/外部存储） ──────────

type noopUserRepo struct{}

func (noopUserRepo) Create(ctx context.Context, u *model.User) error { return nil }
func (noopUserRepo) FindByID(ctx context.Context, id string) (*model.User, error) {
	return nil, nil
}
func (noopUserRepo) FindByEmail(ctx context.Context, e string) (*model.User, error) {
	return nil, nil
}
func (noopUserRepo) FindByGoogleID(ctx context.Context, g string) (*model.User, error) {
	return nil, nil
}
func (noopUserRepo) Update(ctx context.Context, u *model.User) error { return nil }
func (noopUserRepo) UpdatePoints(ctx context.Context, userID string, amount int64) error {
	return nil
}
func (noopUserRepo) UpdatePassword(ctx context.Context, userID, hash string, changedAt interface{}) error {
	return nil
}
func (noopUserRepo) AutoMigrate() error      { return nil }
func (noopUserRepo) Close() error            { return nil }
func (noopUserRepo) SQLDB() (*sql.DB, error) { return nil, nil }

type noopLogRepo struct{}

func (noopLogRepo) Create(ctx context.Context, l *model.AuditLog) error { return nil }
func (noopLogRepo) FindByUser(ctx context.Context, userID string, cursor int64, limit int) ([]model.AuditLog, error) {
	return nil, nil
}
func (noopLogRepo) FindByType(ctx context.Context, eventType string, start, end time.Time, limit int) ([]model.AuditLog, error) {
	return nil, nil
}
func (noopLogRepo) CountByType(ctx context.Context, eventType string, start, end time.Time) (int64, error) {
	return 0, nil
}
func (noopLogRepo) CountByTypeAndResult(ctx context.Context, eventType, result string, start, end time.Time) (int64, error) {
	return 0, nil
}
func (noopLogRepo) Close() error            { return nil }
func (noopLogRepo) SQLDB() (*sql.DB, error) { return nil, nil }

type noopMonitorRepo struct{}

func (noopMonitorRepo) Record(ctx context.Context, m *model.MonitorMetric) error { return nil }
func (noopMonitorRepo) CountByType(ctx context.Context, metricType string, start, end time.Time) (int64, error) {
	return 0, nil
}
func (noopMonitorRepo) SumByType(ctx context.Context, metricType string, start, end time.Time) (float64, error) {
	return 0, nil
}
func (noopMonitorRepo) FindByTimeRange(ctx context.Context, start, end time.Time, limit int) ([]model.MonitorMetric, error) {
	return nil, nil
}
func (noopMonitorRepo) Close() error            { return nil }
func (noopMonitorRepo) SQLDB() (*sql.DB, error) { return nil, nil }

// ────────── 测试构造辅助 ──────────

// buildEventDrivenService 构造仅依赖 mock L2 的 IdleService（不触碰 DB），用于验证事件驱动启停决策。
func buildEventDrivenService(cfg *config.Config, l2 cache.L2Cache) *IdleService {
	mgr := newTestCacheManager(l2)
	repo := repository.NewIdleRepositoryWithCache(nil, mgr, cfg.Idle.ActiveSetShards, false)
	return &IdleService{
		cfg:      cfg,
		idleRepo: repo,
		cacheMgr: mgr,
		logSvc:   common.NewLogService(nil, nil),
	}
}

// testConfigManagerOnce / testConfigManager：测试内复用的配置管理器（仅用于构造缓存 Strategy，
// 不连接真实 Redis；最小可用配置即可通过 Validate）。
var testConfigManagerOnce sync.Once
var testConfigManager *config.Manager

func getTestConfigManager() *config.Manager {
	testConfigManagerOnce.Do(func() {
		dir, err := os.MkdirTemp("", "hc-test-config")
		if err != nil {
			panic(err)
		}
		p := filepath.Join(dir, "config.yaml")
		content := "server:\n  port: 8080\n  mode: test\nratelimit:\n  enabled: false\n"
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			panic(err)
		}
		cm, err := config.LoadManager(p)
		if err != nil {
			panic(err)
		}
		testConfigManager = cm
	})
	return testConfigManager
}

// newTestCacheManager 构造一个带真实 Strategy 的 cache.Manager（L2 注入 mock），
// 使仓库层的缓存读路径（FindActiveByDevice 等经 cacheMgr.Get）可正常工作，且不会连接真实 Redis。
func newTestCacheManager(l2 cache.L2Cache) *cache.Manager {
	strat := cache.NewCacheStrategy(nil, l2, nil, nil, getTestConfigManager(), zap.NewNop())
	return &cache.Manager{L2: l2, Strategy: strat}
}

// buildEventDrivenServiceWithDB 构造带内存 SQLite 的 IdleService，用于集成验证 handleHBExpired 全链路。
func buildEventDrivenServiceWithDB(t *testing.T, cfg *config.Config, l2 cache.L2Cache) *IdleService {
	t.Helper()
	dsn := "file:" + t.Name() + "?mode=memory&cache=shared"
	gdb, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := gdb.AutoMigrate(&model.IdleRecord{}, &model.PointsTransaction{}, &model.PointsOutbox{}, &model.IdleDailyPoints{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	rw := db.NewRWDBFromGORM(gdb)
	mgr := newTestCacheManager(l2)
	repo := repository.NewIdleRepositoryWithCache(rw, mgr, cfg.Idle.ActiveSetShards, false)
	shopRepo := repository.NewShopRepositoryWithCache(rw, mgr)
	return &IdleService{
		cfg:        cfg,
		idleRepo:   repo,
		userRepo:   noopUserRepo{},
		shopRepo:   shopRepo,
		businessDB: rw,
		points:     common.NewPointsOutboxApplier(repository.NewPointsOutboxRepository(gdb), noopUserRepo{}, nil, 0),
		cacheMgr:   mgr,
		logSvc:     common.NewLogService(noopLogRepo{}, noopMonitorRepo{}),
	}
}

func insertActiveRecord(t *testing.T, svc *IdleService, user, dev string, start time.Time) {
	t.Helper()
	rec := &model.IdleRecord{
		UserID:          user,
		DeviceID:        dev,
		StartTime:       start,
		LastHeartbeatAt: &start,
		Status:          "active",
	}
	// 注意：生产 IdleRepository.Create 使用 SQLite 不支持的 ON CONFLICT ... WHERE 子句（面向 PG/MySQL），
	// 测试用内存 SQLite 直接经 gorm 插入，规避该语法差异。
	if err := svc.businessDB.Write(context.Background()).Create(rec).Error; err != nil {
		t.Fatalf("create active record: %v", err)
	}
}

func queryIdleRecord(t *testing.T, svc *IdleService, user, dev string) *model.IdleRecord {
	t.Helper()
	var rec model.IdleRecord
	err := svc.businessDB.Write(context.Background()).
		Where("user_id = ? AND device_id = ?", user, dev).First(&rec).Error
	if err != nil {
		t.Fatalf("query idle record: %v", err)
	}
	return &rec
}

func baseIdleCfg() *config.Config {
	return &config.Config{
		Cache: config.CacheConfig{
			L2: config.L2CacheConfig{DB: 0, Cluster: config.ClusterConfig{Enabled: false}},
		},
		Idle: config.IdleConfig{
			EventDrivenSettle: true,
			HeartbeatInterval: 30 * time.Second,
			TimeoutThreshold:  5 * time.Minute,
			PointsPerMinute:   10,
			ActiveSetShards:   256,
		},
	}
}

// ────────── StartEventDriven 前置决策测试 ──────────

func TestStartEventDrivenGuards(t *testing.T) {
	baseCfg := func() *config.Config {
		return &config.Config{
			Cache: config.CacheConfig{
				L2: config.L2CacheConfig{DB: 0, Cluster: config.ClusterConfig{Enabled: false}},
			},
			Idle: config.IdleConfig{
				EventDrivenSettle: true,
				HeartbeatInterval: time.Minute,
				TimeoutThreshold:  5 * time.Minute,
				ActiveSetShards:   256,
			},
		}
	}

	t.Run("redis_unavailable", func(t *testing.T) {
		cfg := baseCfg()
		l2 := &mockL2{}
		mgr := &cache.Manager{L2: nil}
		repo := repository.NewIdleRepositoryWithCache(nil, mgr, cfg.Idle.ActiveSetShards, false)
		svc := &IdleService{cfg: cfg, idleRepo: repo, cacheMgr: mgr}
		svc.StartEventDriven(context.Background())
		if l2.subscribeCalled {
			t.Fatal("should not subscribe when Redis unavailable")
		}
		if svc.eventCancel != nil {
			t.Fatal("eventCancel should be nil when Redis unavailable")
		}
	})

	t.Run("disabled_by_config", func(t *testing.T) {
		cfg := baseCfg()
		cfg.Idle.EventDrivenSettle = false
		l2 := &mockL2{}
		svc := buildEventDrivenService(cfg, l2)
		svc.StartEventDriven(context.Background())
		if l2.subscribeCalled || l2.configSetCalled {
			t.Fatal("should not subscribe when event_driven_settle=false")
		}
		if svc.eventCancel != nil {
			t.Fatal("eventCancel should be nil when disabled by config")
		}
	})

	t.Run("cluster_mode_auto_disabled", func(t *testing.T) {
		cfg := baseCfg()
		cfg.Cache.L2.Cluster.Enabled = true
		l2 := &mockL2{}
		svc := buildEventDrivenService(cfg, l2)
		svc.StartEventDriven(context.Background())
		if l2.subscribeCalled || l2.configSetCalled {
			t.Fatal("should not subscribe/enable notify in Redis Cluster mode (cross-node unreliable)")
		}
		if svc.eventCancel != nil {
			t.Fatal("eventCancel should be nil in cluster mode")
		}
	})

	t.Run("enabled_subscribes", func(t *testing.T) {
		cfg := baseCfg()
		l2 := &mockL2{subDone: make(chan struct{})}
		svc := buildEventDrivenService(cfg, l2)
		svc.StartEventDriven(context.Background())

		// Subscribe 在 goroutine 内异步执行，等待其完成（带超时，避免时序竞态）
		select {
		case <-l2.subDone:
		case <-time.After(2 * time.Second):
			t.Fatal("Subscribe not invoked within timeout")
		}

		if !l2.configSetCalled {
			t.Fatal("should call ConfigSet to enable notify-keyspace-events")
		}
		if l2.configSetKey != "notify-keyspace-events" || l2.configSetValue != "Ex" {
			t.Fatalf("ConfigSet wrong: key=%q value=%q", l2.configSetKey, l2.configSetValue)
		}
		if !l2.subscribeCalled {
			t.Fatal("should subscribe to expired channel")
		}
		if !strings.Contains(l2.subscribeChannel, "__keyevent@0__:expired") {
			t.Fatalf("subscribe channel wrong: %q", l2.subscribeChannel)
		}
		if svc.eventCancel == nil {
			t.Fatal("eventCancel should be set after subscribe")
		}

		// StopEventDriven 应取消订阅并清空 eventCancel（优雅关闭无泄漏）
		svc.StopEventDriven()
		if svc.eventCancel != nil {
			t.Fatal("eventCancel should be nil after StopEventDriven")
		}
	})
}

// TestHandleHBExpiredInvalidKey 验证：解析失败的过期事件（非心跳 key）应早返回，
// 不会 panic 或触发任何 Redis/DB 操作（事件驱动结算的健壮性）。
func TestHandleHBExpiredInvalidKey(t *testing.T) {
	svc := buildEventDrivenService(baseIdleCfg(), &mockL2{})
	svc.handleHBExpired("some:unrelated:key")
	svc.handleHBExpired("")
}

// TestHandleHBExpiredBranches 集成验证事件驱动结算的三条核心分支：
//  1. 会话已不存在 → 清理悬挂的心跳 key 与活跃集合成员；
//  2. 宽限期内（刚启动未首跳）→ 重新触摸心跳 key，不结算；
//  3. 正常超时 → 走幂等 settleSession 结算，并清理心跳 key 与活跃集合成员。
func TestHandleHBExpiredBranches(t *testing.T) {
	cfg := baseIdleCfg()

	t.Run("session_missing_cleans_dangling", func(t *testing.T) {
		l2 := &mockL2{}
		svc := buildEventDrivenServiceWithDB(t, cfg, l2)
		// 不插入任何记录，直接投递过期事件
		svc.handleHBExpired("idle:active:idle:hb:uMissing:dMissing")

		if len(l2.deletes) == 0 {
			t.Fatal("should delete dangling heartbeat key when session missing")
		}
		if len(l2.srems) == 0 {
			t.Fatal("should remove dangling member from active set when session missing")
		}
	})

	t.Run("grace_period_renews_heartbeat", func(t *testing.T) {
		l2 := &mockL2{}
		svc := buildEventDrivenServiceWithDB(t, cfg, l2)
		// StartTime=now → 处于宽限期（2*heartbeat_interval）内
		insertActiveRecord(t, svc, "uGrace", "dGrace", time.Now())

		svc.handleHBExpired("idle:active:idle:hb:uGrace:dGrace")

		if len(l2.sets) == 0 {
			t.Fatal("should re-touch heartbeat key within grace period")
		}
		rec := queryIdleRecord(t, svc, "uGrace", "dGrace")
		if rec.Status != "active" {
			t.Fatalf("session should remain active during grace period, got status=%q", rec.Status)
		}
	})

	t.Run("timeout_settles_and_cleans", func(t *testing.T) {
		l2 := &mockL2{}
		svc := buildEventDrivenServiceWithDB(t, cfg, l2)
		// StartTime 远早于宽限期 → 触发结算
		insertActiveRecord(t, svc, "uTimeout", "dTimeout", time.Now().Add(-1*time.Hour))

		svc.handleHBExpired("idle:active:idle:hb:uTimeout:dTimeout")

		rec := queryIdleRecord(t, svc, "uTimeout", "dTimeout")
		if rec.Status != "timeout" {
			t.Fatalf("session should be settled as timeout, got status=%q", rec.Status)
		}
		if rec.EndTime == nil {
			t.Fatal("settled session should have EndTime set")
		}
		if rec.PointsEarned <= 0 {
			t.Fatalf("settled session should earn points, got %d", rec.PointsEarned)
		}
		// 结算后清理心跳 key 与活跃集合成员（避免悬挂）
		if len(l2.deletes) == 0 {
			t.Fatal("should delete heartbeat key after settle")
		}
		if len(l2.srems) == 0 {
			t.Fatal("should remove member from active set after settle")
		}
	})
}
