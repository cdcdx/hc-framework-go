package bootstrap

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/cdcdx/hc-framework-go/internal/cache"
	"github.com/cdcdx/hc-framework-go/internal/cluster"
	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/db"
	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/cdcdx/hc-framework-go/internal/scheduler"
)

// ──────────────────────────────────────────────────────
// DSN 解析（脱敏输出）
// ──────────────────────────────────────────────────────

func TestExtractAddrDB(t *testing.T) {
	cases := []struct {
		dsn      string
		wantAddr string
		wantDB   string
	}{
		{"mysql://user:pass@127.0.0.1:3306/hc_business", "127.0.0.1:3306", "hc_business"},
		{"postgres://u:p@db.host:5432/app", "db.host:5432", "app"},
		{"file:/tmp/data/foo.db?cache=shared", "/tmp/data/foo.db", ""},
		{"file:./data/foo.db?cache=shared", "./data/foo.db", ""},
		{"", "", ""},
	}
	for _, c := range cases {
		addr, dbName := extractAddrDB(c.dsn)
		if addr != c.wantAddr || dbName != c.wantDB {
			t.Errorf("extractAddrDB(%q) = (%q,%q), want (%q,%q)",
				c.dsn, addr, dbName, c.wantAddr, c.wantDB)
		}
	}
}

func TestDsnAddr(t *testing.T) {
	if got := dsnAddr("mysql://user:pass@127.0.0.1:3306/hc_business"); got != "127.0.0.1:3306" {
		t.Errorf("dsnAddr = %q, want 127.0.0.1:3306", got)
	}
	if got := dsnAddr(""); got != "" {
		t.Errorf("dsnAddr(\"\") = %q, want empty", got)
	}
}

func TestSafeConnFields(t *testing.T) {
	f := safeConnFields("business", "mysql", "mysql://user:pass@127.0.0.1:3306/hc_business")
	// name + driver + addr + db = 4 个字段
	if len(f) < 4 {
		t.Fatalf("safeConnFields returned %d fields, want >=4", len(f))
	}
}

// ──────────────────────────────────────────────────────
// 扫描间隔钳制 [timeout/3, 2*timeout]
// ──────────────────────────────────────────────────────

func TestIdleScanInterval(t *testing.T) {
	make := func(expr string) time.Duration {
		cfg := &config.Config{}
		cfg.Idle.TimeoutThreshold = 30 * time.Minute
		cfg.Idle.OfflineCheckInterval = expr
		return IdleScanInterval(cfg)
	}
	cases := []struct {
		expr string
		want time.Duration
	}{
		{"*/5", 10 * time.Minute},     // 5min < 下限(10min) → 钳到 10min
		{"*/120", 60 * time.Minute},   // 120min > 上限(60min) → 钳到 60min
		{"*/30", 30 * time.Minute},    // 30min ∈ [10,60] → 不变
		{"*", 10 * time.Minute},       // 1min < 下限 → 钳到 10min
		{"invalid", 30 * time.Minute}, // 解析失败 → 退化为 TimeoutThreshold
	}
	for _, c := range cases {
		if got := make(c.expr); got != c.want {
			t.Errorf("IdleScanInterval(%q) = %v, want %v", c.expr, got, c.want)
		}
	}
}

// ──────────────────────────────────────────────────────
// 数据库回退路径
// ──────────────────────────────────────────────────────

// TestOpenGORMOrFallback_FallsBackToSQLite：mysql 主库 DSN 非法（连接层立即失败、
// 无网络阻塞）时，应按设计回退到 SQLite 并成功打开。
func TestOpenGORMOrFallback_FallsBackToSQLite(t *testing.T) {
	dir := t.TempDir()
	fallback := "file:" + filepath.Join(dir, "fallback.db")
	cfg := &config.Config{} // Migration.AutoMigrate 默认 false

	got, err := openGORMOrFallback("u", "mysql", ":::invalid-dsn:::", fallback,
		[]interface{}{&model.User{}}, cfg, db.PoolConfig{})
	if err != nil {
		t.Fatalf("fallback open failed: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil *gorm.DB after fallback")
	}
}

// TestOpenGORMOrFallback_SQLitePrimaryFailsNoFallback：driver==sqlite 时主库失败
// 不应再回退（避免用 fallback 掩盖配置错误），直接返回错误。
func TestOpenGORMOrFallback_SQLitePrimaryFailsNoFallback(t *testing.T) {
	cfg := &config.Config{}
	// /etc/hosts 是普通文件，其子目录不可创建 → 主库打开必失败
	_, err := openGORMOrFallback("u", "sqlite", "file:/etc/hosts/x/test.db",
		"file:/tmp/should-not-be-used.db", []interface{}{&model.User{}}, cfg, db.PoolConfig{})
	if err == nil {
		t.Fatal("expected error for unreachable sqlite path (no fallback for sqlite)")
	}
}

// TestRwOpenGORM_SQLite：直接以 sqlite 打开单主库连接。
func TestRwOpenGORM_SQLite(t *testing.T) {
	dir := t.TempDir()
	dsn := "file:" + filepath.Join(dir, "ok.db")
	rw, err := rwOpenGORM("x", "sqlite", dsn, []interface{}{&model.User{}}, &config.Config{}, db.PoolConfig{})
	if err != nil {
		t.Fatalf("rwOpenGORM sqlite failed: %v", err)
	}
	if rw == nil || rw.Master() == nil {
		t.Fatal("expected non-nil RWDB with master")
	}
	if err := rw.Close(); err != nil {
		t.Fatalf("rw.Close failed: %v", err)
	}
}

// ──────────────────────────────────────────────────────
// 阶段方法异常路径（引入接口/工厂桩后的可测试性验证，对应 13 文档 §3.34）
// ──────────────────────────────────────────────────────

// TestInitCache_FactoryError：cache 工厂返回错误时，initCache 应立即透传错误、
// 且不设置 cacheMgr（避免 middleware 注入 nil 检查器）。
func TestInitCache_FactoryError(t *testing.T) {
	a := &App{
		zlog: zap.NewNop(),
		cacheFactory: func(_ *config.Manager, _ *zap.Logger) (*cache.Manager, error) {
			return nil, errors.New("redis down")
		},
	}
	if err := a.initCache(); err == nil {
		t.Fatal("expected error when cache factory fails, got nil")
	}
	if a.cacheMgr != nil {
		t.Error("cacheMgr should remain nil after initCache failure")
	}
}

// fakeScheduler 是 jobScheduler 的记录型桩：捕获注册的任务名，Start/Stop 仅置位标记，
// 不真正启动 goroutine，因此 initScheduler 内注册的任务闭包不会被触发（无需真实 idleSvc/taskSvc）。
type fakeScheduler struct {
	intervalJobs []string
	periodJobs   []string
	started      bool
	stopped      bool
}

func (f *fakeScheduler) SetLogger(scheduler.Logger)       {}
func (f *fakeScheduler) SetMetricsHook(scheduler.Metrics) {}
func (f *fakeScheduler) AddIntervalJob(name string, _ time.Duration, _ scheduler.Job) {
	f.intervalJobs = append(f.intervalJobs, name)
}
func (f *fakeScheduler) AddPeriodJob(name string, _ time.Duration, _ func() string, _ scheduler.Job) {
	f.periodJobs = append(f.periodJobs, name)
}
func (f *fakeScheduler) Start(context.Context) { f.started = true }
func (f *fakeScheduler) Stop()                 { f.stopped = true }

// TestInitScheduler_RegistersJobs：initScheduler 应向调度器注册 4 个周期任务
// （idle-timeout-scan / daily·weekly task reset / event-dedup-cleanup），且调用 Start。
func TestInitScheduler_RegistersJobs(t *testing.T) {
	fake := &fakeScheduler{}
	a := &App{
		cfg:          &config.Config{},
		zlog:         zap.NewNop(),
		schedFactory: func() jobScheduler { return fake },
	}
	if err := a.initScheduler(); err != nil {
		t.Fatalf("initScheduler returned error: %v", err)
	}
	if !fake.started {
		t.Error("expected scheduler.Start to be called")
	}

	wantInterval := map[string]bool{"idle-timeout-scan": false, "event-dedup-cleanup": false}
	for _, n := range fake.intervalJobs {
		if _, ok := wantInterval[n]; ok {
			wantInterval[n] = true
		}
	}
	for n, found := range wantInterval {
		if !found {
			t.Errorf("interval job %q not registered (got %v)", n, fake.intervalJobs)
		}
	}

	wantPeriod := map[string]bool{"daily-task-reset": false, "weekly-task-reset": false}
	for _, n := range fake.periodJobs {
		if _, ok := wantPeriod[n]; ok {
			wantPeriod[n] = true
		}
	}
	for n, found := range wantPeriod {
		if !found {
			t.Errorf("period job %q not registered (got %v)", n, fake.periodJobs)
		}
	}
}

// ──────────────────────────────────────────────────────
// 调度器任务闭包的 Leader 门控（3.35 抽取的纯函数）
// ──────────────────────────────────────────────────────

// 以下桩实现 3.35 引入的小接口，仅记录是否调用、可设定返回值/是否 Leader。

type stubLeader struct{ isLeader bool }

func (s stubLeader) IsLeader() bool { return s.isLeader }

type stubIdleScan struct {
	sharding bool
	count    int
}

func (s *stubIdleScan) ScanShardingEnabled() bool { return s.sharding }
func (s *stubIdleScan) ScanAndSettleTimeout(context.Context) (int, error) {
	return s.count, nil
}

// stubTask 同时实现 taskResetter 与 taskDedupCleaner（两接口方法集为其超集）。
type stubTask struct {
	resetCalls int
	dedupCalls int
}

func (s *stubTask) ResetPeriod(context.Context, string) (int64, error) {
	s.resetCalls++
	return int64(s.resetCalls), nil
}
func (s *stubTask) CleanupDedup(context.Context, time.Duration) (int64, error) {
	s.dedupCalls++
	return int64(s.dedupCalls), nil
}

// TestIdleTimeoutScan_Gating：验证「非分片 + 非 Leader → 早退」以及分片/无选主时正常扫描。
func TestIdleTimeoutScan_Gating(t *testing.T) {
	// 非分片 + 非 Leader → 不扫描
	idle := &stubIdleScan{sharding: false}
	if n, _ := idleTimeoutScan(context.Background(), idle, stubLeader{isLeader: false}); n != 0 {
		t.Errorf("non-sharding non-leader should early-return, got n=%d", n)
	}
	// 非分片 + Leader → 扫描
	idle = &stubIdleScan{sharding: false, count: 7}
	if n, _ := idleTimeoutScan(context.Background(), idle, stubLeader{isLeader: true}); n != 7 {
		t.Errorf("leader should scan, got n=%d", n)
	}
	// 分片模式（与选主无关）→ 即使非 Leader 也扫描
	idle = &stubIdleScan{sharding: true, count: 3}
	if n, _ := idleTimeoutScan(context.Background(), idle, stubLeader{isLeader: false}); n != 3 {
		t.Errorf("sharding mode should scan regardless of leader, got n=%d", n)
	}
	// 无选主（leader==nil）→ 扫描
	idle = &stubIdleScan{sharding: false, count: 5}
	if n, _ := idleTimeoutScan(context.Background(), idle, nil); n != 5 {
		t.Errorf("no leader election should scan, got n=%d", n)
	}
}

// TestSchedulerLeader_NilUnderlyingPointer：复现并验证 13 §3.47 的「nil 底层指针接口」陷阱。
// 当未启用选主（Idle.LeaderElection.Enabled==false）或 L2 不可用降级时，
// a.leader 为零值 (*cluster.LeaderElector)(nil)。该 nil 指针赋给 leaderElector 接口后
// 接口非空（仅底层指针为 nil），若门控函数直接调用 IsLeader 会 RL　ock 踩 nil panic。
// 修复后（IsLeader nil 守卫 + 调用方仅非 nil 才入接口）：三个门控函数应安全降级为「正常执行」而不 panic。
func TestSchedulerLeader_NilUnderlyingPointer(t *testing.T) {
	var nilLE *cluster.LeaderElector // 底层 nil 指针；赋给接口后接口非空但底层 nil

	// idleTimeoutScan：无选主语义下应正常扫描（n==count、err==nil，且不得 panic）。
	idle := &stubIdleScan{sharding: false, count: 5}
	if n, err := idleTimeoutScan(context.Background(), idle, nilLE); err != nil || n != 5 {
		t.Errorf("idleTimeoutScan with nil underlying pointer: got n=%d err=%v, want n=5 err=nil", n, err)
	}

	// resetPeriodIfLeader：无选主语义下应执行重置。
	task := &stubTask{}
	if _, err := resetPeriodIfLeader(context.Background(), task, nilLE, "daily"); err != nil || task.resetCalls != 1 {
		t.Errorf("resetPeriodIfLeader with nil underlying pointer: got calls=%d err=%v, want calls=1", task.resetCalls, err)
	}

	// cleanupDedupIfLeader：无选主语义下应执行清理。
	dedup := &stubTask{}
	if _, err := cleanupDedupIfLeader(context.Background(), dedup, nilLE, 7*24*time.Hour); err != nil || dedup.dedupCalls != 1 {
		t.Errorf("cleanupDedupIfLeader with nil underlying pointer: got calls=%d err=%v, want calls=1", dedup.dedupCalls, err)
	}
}

// TestResetPeriodIfLeader_Gating：验证每日/每周重置仅在「无选主或 Leader」时执行。
func TestResetPeriodIfLeader_Gating(t *testing.T) {
	task := &stubTask{}
	if _, err := resetPeriodIfLeader(context.Background(), task, nil, "daily"); err != nil || task.resetCalls != 1 {
		t.Error("no leader election should execute reset")
	}
	task = &stubTask{}
	if _, err := resetPeriodIfLeader(context.Background(), task, stubLeader{isLeader: true}, "daily"); err != nil || task.resetCalls != 1 {
		t.Error("leader should execute reset")
	}
	task = &stubTask{}
	if _, err := resetPeriodIfLeader(context.Background(), task, stubLeader{isLeader: false}, "daily"); err != nil || task.resetCalls != 0 {
		t.Error("non-leader should early-return without reset")
	}
}

// TestCleanupDedupIfLeader_Gating：验证去重清理仅在「无选主或 Leader」时执行。
func TestCleanupDedupIfLeader_Gating(t *testing.T) {
	task := &stubTask{}
	if _, err := cleanupDedupIfLeader(context.Background(), task, nil, 7*24*time.Hour); err != nil || task.dedupCalls != 1 {
		t.Error("no leader election should execute cleanup")
	}
	task = &stubTask{}
	if _, err := cleanupDedupIfLeader(context.Background(), task, stubLeader{isLeader: true}, 7*24*time.Hour); err != nil || task.dedupCalls != 1 {
		t.Error("leader should execute cleanup")
	}
	task = &stubTask{}
	if _, err := cleanupDedupIfLeader(context.Background(), task, stubLeader{isLeader: false}, 7*24*time.Hour); err != nil || task.dedupCalls != 0 {
		t.Error("non-leader should early-return without cleanup")
	}
}

// ──────────────────────────────────────────────────────
// Databases.Close 统一释放（3.37：修复 monitor/log/login 连接泄漏）
// ──────────────────────────────────────────────────────

// 以下桩实现 repository 三个此前缺 Close 的接口，仅记录 Close 调用。
type closeCountRepo struct{ closeCalls int }

func (c *closeCountRepo) Create(context.Context, *model.AuditLog) error { return nil }
func (c *closeCountRepo) FindByUser(context.Context, string, int64, int) ([]model.AuditLog, error) {
	return nil, nil
}
func (c *closeCountRepo) FindByType(context.Context, string, time.Time, time.Time, int) ([]model.AuditLog, error) {
	return nil, nil
}
func (c *closeCountRepo) CountByType(context.Context, string, time.Time, time.Time) (int64, error) {
	return 0, nil
}
func (c *closeCountRepo) Close() error            { c.closeCalls++; return nil }
func (c *closeCountRepo) SQLDB() (*sql.DB, error) { return nil, nil }

type errLogRepo struct{ closeCountRepo }

func (e *errLogRepo) Close() error { return errors.New("log close failed") }

type closeCountMonitor struct{ closeCalls int }

func (c *closeCountMonitor) Record(context.Context, *model.MonitorMetric) error { return nil }
func (c *closeCountMonitor) CountByType(context.Context, string, time.Time, time.Time) (int64, error) {
	return 0, nil
}
func (c *closeCountMonitor) SumByType(context.Context, string, time.Time, time.Time) (float64, error) {
	return 0, nil
}
func (c *closeCountMonitor) FindByTimeRange(context.Context, time.Time, time.Time, int) ([]model.MonitorMetric, error) {
	return nil, nil
}
func (c *closeCountMonitor) Close() error            { c.closeCalls++; return nil }
func (c *closeCountMonitor) SQLDB() (*sql.DB, error) { return nil, nil }

type closeCountLogin struct{ closeCalls int }

func (c *closeCountLogin) Create(context.Context, *model.LoginRecord) error        { return nil }
func (c *closeCountLogin) CreateBatch(context.Context, []*model.LoginRecord) error { return nil }
func (c *closeCountLogin) Close() error                                            { c.closeCalls++; return nil }
func (c *closeCountLogin) SQLDB() (*sql.DB, error)                                 { return nil, nil }

// TestDatabases_Close：Databases.Close 释放 monitor/log/login 三类连接，
// 且 BusinessRW/UserRepo 为 nil 时不 panic（向后兼容）。
func TestDatabases_Close(t *testing.T) {
	mon := &closeCountMonitor{}
	log := &closeCountRepo{}
	login := &closeCountLogin{}
	dbs := &Databases{MonitorRepo: mon, LogRepo: log, LoginRepo: login}
	if err := dbs.Close(); err != nil {
		t.Fatalf("Databases.Close returned error: %v", err)
	}
	if mon.closeCalls != 1 || log.closeCalls != 1 || login.closeCalls != 1 {
		t.Errorf("expected each repo Close called once, got monitor=%d log=%d login=%d",
			mon.closeCalls, log.closeCalls, login.closeCalls)
	}
}

// TestDatabases_Close_AggregatesErrors：单个 repo Close 失败应被聚合返回，
// 且其余 repo 仍被关闭（不阻断资源释放）。
func TestDatabases_Close_AggregatesErrors(t *testing.T) {
	mon := &closeCountMonitor{}
	fail := &errLogRepo{}
	login := &closeCountLogin{}
	dbs := &Databases{MonitorRepo: mon, LogRepo: fail, LoginRepo: login}
	if err := dbs.Close(); err == nil {
		t.Fatal("expected aggregated error when a repo Close fails")
	}
	if mon.closeCalls != 1 || login.closeCalls != 1 {
		t.Errorf("other repos should still close, got monitor=%d login=%d", mon.closeCalls, login.closeCalls)
	}
}

// TestValidateInsecureMQ 验证生产环境误用 insecure MQ 的硬卡点（13 §3.40 方向2）。
func TestValidateInsecureMQ(t *testing.T) {
	base := func() *config.Config {
		return &config.Config{MQ: config.MQConfig{Type: "memory"}}
	}
	// 1) 单副本 dev（memory）：不阻断，由 mq 包既有 WARN 提示
	if err := validateInsecureMQ(base()); err != nil {
		t.Fatalf("single-replica dev should pass, got %v", err)
	}
	// 2) 多副本（pod_total>1）+ memory：拒绝启动
	multi := base()
	multi.Idle.ScanSharding.PodTotal = 3
	if err := validateInsecureMQ(multi); err == nil {
		t.Fatal("multi-replica with memory MQ should be rejected")
	}
	// 3) 启用 Leader 选举 + memory：拒绝启动
	leader := base()
	leader.Idle.LeaderElection.Enabled = true
	if err := validateInsecureMQ(leader); err == nil {
		t.Fatal("leader-election with memory MQ should be rejected")
	}
	// 4) 真实 broker（kafka）+ 多副本：允许
	real := base()
	real.MQ.Type = "kafka"
	real.Idle.ScanSharding.PodTotal = 3
	if err := validateInsecureMQ(real); err != nil {
		t.Fatalf("real broker multi-replica should pass, got %v", err)
	}
	// 5) 逃逸 hatch：APP_MQ_ALLOW_INSECURE=true 强制放行（仅限排障）
	t.Setenv("APP_MQ_ALLOW_INSECURE", "true")
	escape := base()
	escape.Idle.ScanSharding.PodTotal = 3
	if err := validateInsecureMQ(escape); err != nil {
		t.Fatalf("allow-insecure escape should pass, got %v", err)
	}
}
