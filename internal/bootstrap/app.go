package bootstrap

import (
	"context"
	"fmt"
	"github.com/cdcdx/hc-framework-go/internal/service/auth"
	"github.com/cdcdx/hc-framework-go/internal/service/common"
	"github.com/cdcdx/hc-framework-go/internal/service/idle"
	"github.com/cdcdx/hc-framework-go/internal/service/shop"
	"github.com/cdcdx/hc-framework-go/internal/service/task"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/cdcdx/hc-framework-go/internal/cache"
	"github.com/cdcdx/hc-framework-go/internal/cluster"
	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/db"
	"github.com/cdcdx/hc-framework-go/internal/event"
	"github.com/cdcdx/hc-framework-go/internal/handler"
	"github.com/cdcdx/hc-framework-go/internal/metrics"
	"github.com/cdcdx/hc-framework-go/internal/middleware"
	"github.com/cdcdx/hc-framework-go/internal/mq"
	"github.com/cdcdx/hc-framework-go/internal/repository"
	"github.com/cdcdx/hc-framework-go/internal/router"
	"github.com/cdcdx/hc-framework-go/internal/scheduler"
	"github.com/cdcdx/hc-framework-go/internal/trace"
	"github.com/cdcdx/hc-framework-go/pkg/logger"
	"github.com/cdcdx/hc-framework-go/pkg/mail"
)

// pointsRelayer 是积分 Outbox 补偿器对 bootstrap 暴露的最小契约。其具体类型在 service
// 包内未导出，故用接口解耦，便于在此持有并调用 RelayLoop 与 ApplyPointsAdjust。
type pointsRelayer interface {
	RelayLoop(ctx context.Context, tick, batchWindow time.Duration)
	ApplyPointsAdjust(ctx context.Context, msg *event.Message) error
}

// jobScheduler 是 bootstrap 对定时调度器的最小契约，便于在单测中注入记录型桩，
// 校验 initScheduler 的任务注册清单。*scheduler.Scheduler 天然满足该接口。
type jobScheduler interface {
	SetLogger(l scheduler.Logger)
	SetMetricsHook(m scheduler.Metrics)
	AddIntervalJob(name string, interval time.Duration, fn scheduler.Job)
	AddPeriodJob(name string, poll time.Duration, periodFn func() string, fn scheduler.Job)
	Start(ctx context.Context)
	Stop()
}

// App 持有进程生命周期所需的全部组件，并编排启动、运行与优雅关闭。
// 把原先堆在 cmd/server/main.go 的「上帝函数」下沉到这里，让 main 只剩薄入口。
type App struct {
	cfg    *config.Config
	cfgMgr *config.Manager
	zlog   *zap.Logger

	dbs           *Databases
	businessDB    *db.RWDB
	userRepo      repository.UserRepository
	cacheMgr      *cache.Manager
	eventProducer mq.Producer
	eventConsumer mq.Consumer
	leader        *cluster.LeaderElector
	sched         jobScheduler

	authSvc       *auth.AuthService
	idleSvc       *idle.IdleService
	taskSvc       *task.TaskService
	shopSvc       *shop.ShopService
	logSvc        *common.LogService
	pointsApplier pointsRelayer
	wsHub         *handler.WebSocketHub

	// 以下工厂字段为可测试性引入的最小接缝：单测可注入桩，覆盖 initCache / initScheduler 异常路径。
	cacheFactory func(cm *config.Manager, log *zap.Logger) (*cache.Manager, error)
	schedFactory func() jobScheduler
}

// Run 应用入口：加载配置 → 初始化 → 启动后台任务与 HTTP 服务 → 阻塞至信号 → 优雅关闭。
// 返回进程退出码：0 正常退出，1 启动或运行期致命错误。
// Run 应用入口：加载配置 → 初始化 → 启动后台任务与 HTTP 服务 → 阻塞至信号 → 优雅关闭。
// 返回进程退出码：0 正常退出，1 启动或运行期致命错误。
// configPath 由 main 解析命令行后传入（flag 解析统一收敛到 main，避免重复/顺序冲突）。
func Run(configPath string) int {
	a, err := newApp(configPath)
	if err != nil {
		// 配置加载在 logger 初始化前失败，已在 newApp 内打印到 stderr
		return 1
	}
	if err := a.run(); err != nil {
		a.zlog.Error("Application exited with error", zap.Error(err))
		return 1
	}
	return 0
}

// newApp 加载配置与日志（早于其它组件），构造 App 骨架。
func newApp(cfgPath string) (*App, error) {
	cfgMgr, err := config.LoadManager(cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		return nil, err
	}
	cfg := cfgMgr.Get()

	logger.Init(cfg.Logging.Level, cfg.Logging.Format)
	zlog := logger.L()

	return &App{
		cfg:          cfg,
		cfgMgr:       cfgMgr,
		zlog:         zlog,
		cacheFactory: cache.NewManager,
		schedFactory: func() jobScheduler { return scheduler.New() },
	}, nil
}

// run 是启动总编排：按依赖顺序调用各阶段初始化方法，启动 HTTP 后阻塞至退出信号，
// 最后交由 shutdown 优雅关闭。每个阶段方法都只做「一件事」，便于单独审视与测试。
func (a *App) run() error {
	cfg := a.cfg
	zlog := a.zlog

	zlog.Info("Starting HC Framework Server",
		zap.Int("port", cfg.Server.Port),
		zap.String("mode", cfg.Server.Mode),
	)

	// 1. 启动配置热更新监听（SIGHUP 信号触发重载；文件监听在 LoadManager 内已注册）
	a.cfgMgr.Watch(context.Background())

	// 1.5 初始化链路追踪 Provider（OTel 兼容，W3C traceparent）
	a.initTracing()

	// 2. 初始化四个数据库（逻辑见 InitDatabases）
	dbs, err := InitDatabases(cfg)
	if err != nil {
		zlog.Error("Failed to init databases", zap.Error(err))
		return err
	}
	a.dbs = dbs
	a.businessDB = dbs.BusinessRW
	a.userRepo = dbs.UserRepo
	// 注：businessDB / userRepo / cacheMgr / Kafka 等资源的释放集中到 run 末尾的
	// 优雅关闭段按序执行（见文件底部 shutdown 流程），不再用分散的 defer——
	// 以便统一计时、带日志与超时兜底，避免"真正的关闭动作藏在静默 defer 里"造成退出观感变慢。

	// 3. 初始化 JWT 管理器
	if err := a.initJWT(); err != nil {
		return err
	}

	// 3.5 初始化缓存管理器（L1 + L2 + Bloom + HotKey），并注入为 Token 黑名单检查器
	if err := a.initCache(); err != nil {
		return err
	}

	// 4+5. 初始化 Services（LogService / 事件生产者 / 邮件 / 锁定存储 / Auth /
	//     积分 Outbox / Idle·Task·Shop / 事件消费者 / 选主）。
	//     bgCtx 用于取消所有"监听/轮询类"后台协程，使优雅关闭时它们能立即退出，
	//     而非依赖 context.Background() 永不取消导致被硬杀。
	bgCtx, bgCancel := context.WithCancel(context.Background())
	defer bgCancel()
	if err := a.initServices(bgCtx); err != nil {
		return err
	}

	// 6. 种子数据 + 索引 + 回填 + 事件驱动结算 + 心跳落库 + 死分片接管 + 副本发现
	a.startBackgroundTasks(bgCtx)

	// 6.6 初始化并启动定时任务调度器（挂机超时结算 / 每日·每周任务重置 / 事件去重清理）
	if err := a.initScheduler(); err != nil {
		return err
	}

	// 6.7 初始化 WebSocket Hub（用于实时推送：挂机心跳替代、积分/任务通知）
	a.wsHub = handler.NewWebSocketHub(a.zlog)
	go a.wsHub.Run(bgCtx) // bgCtx 取消时 Hub 自动关闭所有连接

	// 7. 构建依赖注入容器
	deps := &handler.Dependencies{
		Cfg:         cfg,
		CfgMgr:      a.cfgMgr,
		BusinessDB:  a.businessDB,
		UserRepo:    dbs.UserRepo,
		MonitorRepo: dbs.MonitorRepo,
		LogRepo:     dbs.LogRepo,
		AuthSvc:     a.authSvc,
		IdleSvc:     a.idleSvc,
		TaskSvc:     a.taskSvc,
		ShopSvc:     a.shopSvc,
		LogSvc:      a.logSvc,
		CacheMgr:    a.cacheMgr,
		WSHub:       a.wsHub,
	}

	// 8. 初始化路由
	r := router.Setup(deps)

	// 9. 启动 HTTP Server（另起 goroutine ListenAndServe，主协程继续阻塞至退出信号）
	//    启动成功后标记就绪（serving=true），使 /ready 探针返回 200、K8s 把本 Pod 纳入 Service 端点。
	handler.SetServing(true)
	srv, err := a.startHTTP(r)
	if err != nil {
		return err
	}

	// 优雅关闭
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	a.shutdown(srv, bgCancel)
	return nil
}

// initTracing 初始化 OTel 兼容的链路追踪 Provider（HTTP 服务前必须就绪，使 span 能注入 traceparent）。
// exporter 由配置决定：otlp（HTTP/JSON → Tempo/Jaeger）/ stdout|log（结构化日志）/ none（关闭）。
func (a *App) initTracing() {
	cfg := a.cfg
	zlog := a.zlog

	var texporter trace.Exporter
	switch strings.ToLower(cfg.Tracing.Exporter) {
	case "otlp":
		texporter = trace.NewOTLPHTTPExporter(
			cfg.Tracing.OTLP.Endpoint,
			trace.WithInsecure(cfg.Tracing.OTLP.Insecure),
			trace.WithServiceName(cfg.Tracing.ServiceName),
		)
	case "none":
		texporter = trace.NewNoopExporter()
	default: // stdout / log / 空 → 使用默认日志导出器
		texporter = nil
	}
	prov := trace.NewProvider(cfg.Tracing.ServiceName, zlog, texporter)
	if cfg.Tracing.Sampler.Type != "" {
		prov.SetSampler(trace.SamplerConfig{
			Type:      cfg.Tracing.Sampler.Type,
			Rate:      cfg.Tracing.Sampler.Rate,
			ErrorRate: cfg.Tracing.Sampler.ErrorRate,
		})
	}
	trace.SetGlobal(prov)
	zlog.Info("Tracing provider initialized (OTel-compatible, W3C traceparent)",
		zap.String("exporter", cfg.Tracing.Exporter),
		zap.String("service", cfg.Tracing.ServiceName),
	)
}

// initJWT 初始化 JWT 管理器（算法 / 密钥 / issuer / TTL）。
func (a *App) initJWT() error {
	cfg := a.cfg
	if err := auth.InitJWT(
		cfg.Auth.JWT.Algorithm,
		cfg.Auth.JWT.SigningKey,
		cfg.Auth.JWT.PrivateKeyPath,
		cfg.Auth.JWT.PublicKeyPath,
		cfg.Auth.JWT.Issuer,
		cfg.Auth.JWT.AccessTTL,
		cfg.Auth.JWT.RefreshTTL,
	); err != nil {
		a.zlog.Error("Failed to init JWT", zap.Error(err))
		return err
	}
	a.zlog.Info("JWT manager initialized")
	return nil
}

// initCache 初始化缓存管理器（L1 + L2 + Bloom + HotKey）。
// L2 不可用时自动降级，不影响服务启动。缓存管理器同时被注入为 Token 黑名单检查器。
func (a *App) initCache() error {
	cacheMgr, err := a.cacheFactory(a.cfgMgr, a.zlog)
	if err != nil {
		a.zlog.Error("Failed to init cache", zap.Error(err))
		return err
	}
	a.cacheMgr = cacheMgr
	middleware.SetBlacklistChecker(cacheMgr)
	a.zlog.Info("Cache manager initialized")
	return nil
}

// initServices 初始化全部业务服务及其依赖：LogService、事件生产者/消费者、邮件发送器、
func (a *App) initServices(bgCtx context.Context) error {
	if err := validateInsecureMQ(a.cfg); err != nil {
		return err
	}
	a.initLogService()
	eventProducer := a.initEventProducer()
	a.initLockStore(bgCtx)
	a.initMailer()
	a.initAuthService(bgCtx, eventProducer)
	pointsApplier := a.initBusinessServices(eventProducer)
	a.initEventConsumer(bgCtx, pointsApplier)
	a.initLeaderElection(bgCtx)
	return nil
}

func (a *App) initLogService() {
	logSvc := common.NewLogService(a.dbs.LogRepo, a.dbs.MonitorRepo)
	a.logSvc = logSvc
}

func (a *App) initEventProducer() mq.Producer {
	cfg := a.cfg
	zlog := a.zlog
	if strings.EqualFold(strings.TrimSpace(cfg.MQ.Type), "kafka") {
		mq.EnsureTopics(&cfg.MQ.Kafka, zlog.Sugar())
	}
	var eventProducer mq.Producer
	if pub, pubErr := mq.NewProducer(context.Background(), &cfg.MQ, zlog.Sugar()); pubErr != nil {
		zlog.Warn("event producer init failed, falling back to no-op (events will not be published)", zap.Error(pubErr))
		eventProducer = mq.NewNopProducer()
	} else {
		eventProducer = pub
		if mq.IsEnabled(&cfg.MQ) {
			zlog.Info("event producer initialized", zap.String("type", cfg.MQ.Type))
		} else {
			zlog.Info("event producer disabled (no-op); events will not be published via bus",
				zap.String("status", mq.DescribeConfig(&cfg.MQ)))
		}
	}
	a.eventProducer = eventProducer
	zlog.Info("event bus", zap.String("status", mq.DescribeConfig(&cfg.MQ)), zap.Bool("enabled", mq.IsEnabled(&cfg.MQ)))
	return eventProducer
}

func (a *App) initAuthService(ctx context.Context, eventProducer mq.Producer) {
	lockStore := a.initLockStore(ctx)
	mailer := a.initMailer()
	authSvc := auth.NewAuthService(a.cfgMgr, a.dbs.UserRepo, a.logSvc, a.cacheMgr, eventProducer, lockStore, mailer)
	a.authSvc = authSvc
}

func (a *App) initLockStore(ctx context.Context) cache.AccountLockStore {
	if a.cacheMgr.L2Enabled() {
		return a.cacheMgr
	}
	return auth.NewMemoryAccountLockStore(ctx)
}

func (a *App) initMailer() mail.Sender {
	m := a.cfg.Mail
	if !m.Enabled {
		return nil
	}
	if smtpSender := mail.NewSMTPSender(mail.SMTPConfig{
		Enabled:            true,
		Host:               m.SMTP.Host,
		Port:               m.SMTP.Port,
		Username:           m.SMTP.Username,
		Password:           m.SMTP.Password,
		From:               m.SMTP.From,
		FromName:           m.SMTP.FromName,
		UseTLS:             m.SMTP.UseTLS,
		InsecureSkipVerify: m.SMTP.InsecureSkipVerify,
	}); smtpSender != nil {
		a.zlog.Info("Mail sender initialized (SMTP)",
			zap.String("host", m.SMTP.Host),
			zap.Int("port", m.SMTP.Port),
			zap.String("from", m.SMTP.From),
		)
		return smtpSender
	}
	return nil
}

func (a *App) initBusinessServices(eventProducer mq.Producer) pointsRelayer {
	cfg := a.cfg
	logSvc := a.logSvc
	if mq.IsEnabled(&cfg.MQ) {
		a.cacheMgr.SetDegradeSender(NewDegradeSender(eventProducer))
	}
	pointsApplier := common.NewPointsOutboxApplier(
		repository.NewPointsOutboxRepository(a.businessDB.Master()),
		a.dbs.UserRepo,
		eventProducer,
		0,
	)
	a.pointsApplier = pointsApplier
	a.idleSvc = idle.NewIdleService(cfg, a.dbs.UserRepo, a.businessDB, logSvc, a.cacheMgr, eventProducer, pointsApplier)
	a.taskSvc = task.NewTaskService(cfg, a.dbs.UserRepo, a.businessDB, logSvc, a.cacheMgr, pointsApplier)
	a.shopSvc = shop.NewShopService(cfg, a.dbs.UserRepo, a.businessDB, logSvc, a.cacheMgr, eventProducer, pointsApplier)
	return pointsApplier
}

func (a *App) initEventConsumer(bgCtx context.Context, pointsApplier pointsRelayer) {
	cfg := a.cfg
	zlog := a.zlog
	consumer, cErr := mq.NewConsumer(context.Background(), &cfg.MQ, zlog.Sugar())
	if cErr != nil {
		zlog.Warn("event consumer init failed, event-driven progress disabled", zap.Error(cErr))
		return
	}
	a.eventConsumer = consumer
	if !mq.IsEnabled(&cfg.MQ) {
		zlog.Info("event consumer disabled (no-op)", zap.String("status", mq.DescribeConfig(&cfg.MQ)))
		return
	}
	topics := mq.ConsumerTopics(&cfg.MQ)
	taskSvc := a.taskSvc // capture before goroutine
	handlerFn := func(ctx context.Context, msg *event.Message) error {
		switch msg.EventType {
		case event.EventIdleSettled, event.EventShopRedeemed:
			return taskSvc.ApplyEventProgress(ctx, msg)
		case event.EventUserPointsAdjust:
			return pointsApplier.ApplyPointsAdjust(ctx, msg)
		default:
			return nil
		}
	}
	go func() {
		if err := consumer.Subscribe(bgCtx, topics, handlerFn); err != nil {
			zlog.Warn("event consumer stopped", zap.Error(err))
		}
	}()
	zlog.Info("event consumer started", zap.Strings("topics", topics), zap.String("type", cfg.MQ.Type))
}

func (a *App) initLeaderElection(bgCtx context.Context) {
	cfg := a.cfg
	if !cfg.Idle.LeaderElection.Enabled || a.cacheMgr == nil || a.cacheMgr.L2 == nil {
		return
	}
	le := cfg.Idle.LeaderElection
	key := le.LockKey
	if key == "" {
		key = "idle:leader"
	}
	ttl := le.TTL
	if ttl <= 0 {
		ttl = 15 * time.Second
	}
	renewal := le.Renewal
	if renewal <= 0 {
		renewal = ttl / 2
	}
	leader := cluster.NewLeaderElector(a.cacheMgr, key, ttl, renewal)
	// 绑定 bgCtx：优雅关闭时随 bgCancel 退出选主循环并释放锁，避免 Leader 租约残留。
	leader.Start(bgCtx)
	a.leader = leader
	a.zlog.Info("Leader election enabled", zap.String("key", key), zap.Duration("ttl", ttl))
}

// startBackgroundTasks 在核心服务就绪后启动所有「监听/轮询/补偿」类后台协程：
// 积分 Outbox 补偿 relay、种子数据、索引确保、活跃会话回填、事件驱动结算、
// 心跳批量落库、死分片接管、Headless Service 副本发现。
// 这些协程统一由 bgCtx 取消（优雅关闭时随 bgCancel 退出，而非被硬杀）。
func (a *App) startBackgroundTasks(bgCtx context.Context) {
	// 1. 后台补偿循环（最终一致性兜底，独立于 Kafka 是否可用）；随 bgCancel 统一停止。
	go a.pointsApplier.RelayLoop(bgCtx, 5*time.Second, 10*time.Second)
	// 2. 启动期一次性数据准备（种子/索引/回填），同步顺序执行。
	a.initStartupOnce()
	// 3. 启动期事件驱动/轮询后台协程（随 bgCancel 退出）。
	a.startIdleBackground(bgCtx)
}

// initStartupOnce 启动期一次性数据准备：种子数据、确保唯一索引、回填活跃会话集合。
// 这些操作只需在启动期执行一次，失败仅告警不阻断启动（运行时仍可手工触发/自愈）。
func (a *App) initStartupOnce() {
	zlog := a.zlog
	if err := SeedData(a.taskSvc, a.shopSvc); err != nil {
		zlog.Warn("Seed data failed", zap.Error(err))
	}
	// P2-5：存量库补齐库存分桶（幂等），使存量商品也享受分桶写分散，避免单行锁热点。
	if err := a.shopSvc.BackfillStockBuckets(context.Background()); err != nil {
		zlog.Warn("Backfill stock buckets failed", zap.Error(err))
	}
	if err := a.idleSvc.EnsureIndexes(); err != nil {
		zlog.Warn("Idle indexes ensure failed", zap.Error(err))
	}
	if err := a.idleSvc.BackfillActiveSessions(context.Background()); err != nil {
		zlog.Warn("Idle active sessions backfill failed", zap.Error(err))
	}
}

// startIdleBackground 启动事件驱动结算、心跳 flush、死分片接管、副本发现等常驻协程。
// 全部由 bgCtx 取消（优雅关闭时随 bgCancel 退出，而非被硬杀）。
func (a *App) startIdleBackground(bgCtx context.Context) {
	// 6.5.2 启动事件驱动结算（Keyspace Notification）：心跳 key 过期即结算（零轮询）。
	//       集群模式自动禁用、保留轮询 scanner 兜底；Redis 不可用时为 no-op。
	//       绑定 bgCtx：优雅关闭时随 bgCancel 退出订阅，避免 goroutine 泄漏。
	a.idleSvc.StartEventDriven(bgCtx)
	// 启动心跳批量落库定时器（每 Pod 本地聚合，定时批量 flush last_heartbeat_at；未启用时为 no-op）。
	// 绑定 bgCtx：优雅关闭时随 bgCancel 退出 flush 定时器。
	a.idleSvc.StartHeartbeatFlush(bgCtx)

	// 死分片接管协调器：仅当扫描分片 + failover 开启时生效。各 Pod 周期上报存活，
	// Leader 接管失活 Pod 的集合分片，消除“Pod 宕机 → 其分片永不扫描”的可用性缺口。
	// 无选主（leader==nil）时不接管，各 Pod 仅扫自身分片。
	go a.idleSvc.StartScanFailover(bgCtx, func() bool {
		return a.leader != nil && a.leader.IsLeader()
	})
	// Headless Service 动态副本发现（pod_discovery=headless 时生效）：周期性 DNS 解析副本数，
	// 使 HPA 扩缩时各 Pod 自动感知 pod_total，无需手改配置或滚动重启。
	go a.idleSvc.StartPodDiscovery(bgCtx)
}

// ──────────────────────────────────────────────────────
// 调度器任务闭包的可测化辅助
// ──────────────────────────────────────────────────────
// 把「非 Leader 早退」的判定与具体动作从 initScheduler 的闭包里抽成接收小接口参数的
// 纯函数，便于单测锁定 Leader 门控语义，而不必把 App.idleSvc/taskSvc/leader 字段改为
// 接口（那会波及 initServices / SeedData / handler.Dependencies，风险高）。

// leaderElector 是选主协调器对调度器闭包的最小契约。
type leaderElector interface {
	IsLeader() bool
}

// idleScanClient 是 idle 超时扫描对调度器闭包的最小契约。
type idleScanClient interface {
	ScanShardingEnabled() bool
	ScanAndSettleTimeout(ctx context.Context) (int, error)
}

// taskResetter 是周期任务重置对调度器闭包的最小契约。
type taskResetter interface {
	ResetPeriod(ctx context.Context, period string) (int64, error)
}

// taskDedupCleaner 是事件去重清理对调度器闭包的最小契约。
type taskDedupCleaner interface {
	CleanupDedup(ctx context.Context, older time.Duration) (int64, error)
}

// idleTimeoutScan 在非分片且非 Leader 时早退；否则执行超时结算。
// 分片模式（与选主无关）或无选主时均正常扫描。
func idleTimeoutScan(ctx context.Context, idle idleScanClient, leader leaderElector) (int, error) {
	if !idle.ScanShardingEnabled() && leader != nil && !leader.IsLeader() {
		return 0, nil
	}
	return idle.ScanAndSettleTimeout(ctx)
}

// resetPeriodIfLeader 仅当未启用选主或本 Pod 为 Leader 时重置周期进度。
func resetPeriodIfLeader(ctx context.Context, task taskResetter, leader leaderElector, period string) (int64, error) {
	if leader != nil && !leader.IsLeader() {
		return 0, nil
	}
	return task.ResetPeriod(ctx, period)
}

// cleanupDedupIfLeader 仅当未启用选主或本 Pod 为 Leader 时清理去重表。
func cleanupDedupIfLeader(ctx context.Context, task taskDedupCleaner, leader leaderElector, older time.Duration) (int64, error) {
	if leader != nil && !leader.IsLeader() {
		return 0, nil
	}
	return task.CleanupDedup(ctx, older)
}

// flashSaleWarmupClient 是抢购库存预热/对账对调度器闭包的最小契约。
type flashSaleWarmupClient interface {
	WarmupDueFlashSales(ctx context.Context) (int, error)
}

// warmupDueFlashSalesIfLeader 仅当未启用选主或本 Pod 为 Leader 时预热/对账抢购库存。
func warmupDueFlashSalesIfLeader(ctx context.Context, svc flashSaleWarmupClient, leader leaderElector) (int, error) {
	if leader != nil && !leader.IsLeader() {
		return 0, nil
	}
	return svc.WarmupDueFlashSales(ctx)
}

// initScheduler 初始化定时任务调度器并注册全部周期任务：
//   - idle-timeout-scan：挂机超时结算（分片模式各 Pod 扫自身分片；非分片 + 选主启用时仅 Leader 全量扫描）
//   - daily-task-reset / weekly-task-reset：每日/每周任务进度重置（选主启用时仅 Leader 执行）
//   - event-dedup-cleanup：每 6 小时清理 7 天前 event_dedup 记录（选主启用时仅 Leader 执行）
func (a *App) initScheduler() error {
	zlog := a.zlog
	cfg := a.cfg

	sched := a.schedFactory()
	sched.SetLogger(zlog.Sugar())
	// 注入 panic 上报钩子：调度任务 panic 经 metrics.SchedulerJobPanicsTotal 暴露，
	// 与消费侧「panic→计数」规范一致（失败分支在下方各闭包内上报 SchedulerJobFailuresTotal）。
	sched.SetMetricsHook(metrics.SchedulerMetricsAdapter{})
	a.sched = sched

	// 选主协调器：未启用选主（Idle.LeaderElection.Enabled==false）或 L2 不可用时
	// a.leader 为 nil 指针。若直接把该 nil 指针传给 leaderElector 接口参数，会因
	// 「nil 底层指针接口」陷阱（接口非空、底层 nil）导致 leader != nil 误判并调用
	// IsLeader 时 panic（见 13 §3.47）。故仅当非 nil 才赋给接口，否则保持真正 nil 接口。
	var le leaderElector
	if a.leader != nil {
		le = a.leader
	}

	// 挂机超时扫描：按 offline_check_interval（cron 分钟字段）解析，并钳制到
	// [timeout/3, 2*timeout] 之间，兼顾检测延迟与扫描开销。
	// 分片模式下各 Pod 扫描自身分片集合（与选主无关）；非分片 + 选主启用时仅 Leader 全量扫描，
	// 避免“每副本全量”的 O(K×N) 冗余。
	sched.AddIntervalJob("idle-timeout-scan", IdleScanInterval(cfg), func(ctx context.Context) {
		// 非分片 + 非 Leader 早退；分片模式（与选主无关）或无选主时正常扫描。
		// 门控与动作抽到 idleTimeoutScan，便于单测锁定语义（见 bootstrap_test.go）。
		n, sErr := idleTimeoutScan(ctx, a.idleSvc, le)
		if sErr != nil {
			zlog.Warn("Idle timeout scan failed", zap.Error(sErr))
			metrics.SchedulerJobFailuresTotal.WithLabelValues("idle-timeout-scan").Inc()
		} else if n > 0 {
			zlog.Info("Idle timeout settled", zap.Int("count", n))
		}
	})
	// 每日任务进度重置（UTC 周期边界触发）。选主启用时仅 Leader 执行。
	sched.AddPeriodJob("daily-task-reset", time.Minute, func() string {
		return repository.GetCurrentPeriod("daily")
	}, func(ctx context.Context) {
		// 非 Leader 早退；门控抽到 resetPeriodIfLeader（见 bootstrap_test.go）。
		n, rErr := resetPeriodIfLeader(ctx, a.taskSvc, le, "daily")
		if rErr != nil {
			zlog.Warn("Daily task reset failed", zap.Error(rErr))
			metrics.SchedulerJobFailuresTotal.WithLabelValues("daily-task-reset").Inc()
		} else {
			zlog.Info("Daily task progress reset", zap.Int64("cleared", n))
		}
	})
	// 每周任务进度重置（周一 UTC 周期边界触发）。选主启用时仅 Leader 执行。
	sched.AddPeriodJob("weekly-task-reset", time.Minute, func() string {
		return repository.GetCurrentPeriod("weekly")
	}, func(ctx context.Context) {
		n, rErr := resetPeriodIfLeader(ctx, a.taskSvc, le, "weekly")
		if rErr != nil {
			zlog.Warn("Weekly task reset failed", zap.Error(rErr))
			metrics.SchedulerJobFailuresTotal.WithLabelValues("weekly-task-reset").Inc()
		} else {
			zlog.Info("Weekly task progress reset", zap.Int64("cleared", n))
		}
	})
	// 每日积分汇总表回填（本地日边界触发）：批量从 idle_records 算出各用户当日总额写入
	// idle_daily_points，避免「每个用户首笔查询各做一次全表 SUM」的日初风暴（见压测日志分析）。
	// 增量维护（UpsertDailyPoints）已在每次结算时更新 running total；本回填仅补「缺失行」
	// （repo 内 ON CONFLICT DO NOTHING），不覆盖已增量累计值。幂等，多副本并发执行也安全。
	sched.AddPeriodJob("idle-daily-points-backfill", time.Minute, func() string {
		return repository.LocalDayString()
	}, func(ctx context.Context) {
		n, bErr := a.idleSvc.BackfillDailyPoints(ctx)
		if bErr != nil {
			zlog.Warn("Idle daily points backfill failed", zap.Error(bErr))
			metrics.SchedulerJobFailuresTotal.WithLabelValues("idle-daily-points-backfill").Inc()
		} else if n > 0 {
			zlog.Info("Idle daily points backfilled", zap.Int64("rows", n))
		}
	})
	// 事件去重表清理：每 6 小时删除 7 天前的 event_dedup 记录，防止表无限增长。
	// 幂等 event_id 派生自业务主键，过期后不可能再重放，故按 created_at 清理安全。
	// 选主启用时仅 Leader 执行。
	sched.AddIntervalJob("event-dedup-cleanup", 6*time.Hour, func(ctx context.Context) {
		// 非 Leader 早退；门控抽到 cleanupDedupIfLeader（见 bootstrap_test.go）。
		n, cErr := cleanupDedupIfLeader(ctx, a.taskSvc, le, 7*24*time.Hour)
		if cErr != nil {
			zlog.Warn("Event dedup cleanup failed", zap.Error(cErr))
			metrics.SchedulerJobFailuresTotal.WithLabelValues("event-dedup-cleanup").Inc()
		} else if n > 0 {
			zlog.Info("Event dedup records cleaned", zap.Int64("deleted", n))
		}
	})
	// 启动即做一次 best-effort 回填（覆盖「服务中途部署、当日尚未回填」的窗口），不阻塞启动；
	// idleSvc 未初始化（如单测桩）时跳过，避免空指针。
		if a.idleSvc != nil {
		go func() {
			if _, err := a.idleSvc.BackfillDailyPoints(context.Background()); err != nil {
				zlog.Warn("Idle daily points startup backfill failed", zap.Error(err))
			}
		}()
	}
	// 定时抢购库存预热/对账（高并发高可用）：每 15s 由 Leader 扫描「即将开抢/已开始」活动，
	// 以 DB 权威剩余值（limit_qty - sold_qty）强制对齐 Redis 库存，确保开抢瞬间削峰层已就绪，
	// 并在事故（进程崩溃致预扣未回滚等）后自愈消除 Redis 库存漂移。多副本下仅 Leader 执行，
	// 幂等安全（Reconcile 强制覆盖）。无选主时各副本均执行（重复对齐无副作用）。
	sched.AddIntervalJob("flash-sale-warmup", 15*time.Second, func(ctx context.Context) {
		n, wErr := warmupDueFlashSalesIfLeader(ctx, a.shopSvc, le)
		if wErr != nil {
			zlog.Warn("Flash sale warmup failed", zap.Error(wErr))
			metrics.SchedulerJobFailuresTotal.WithLabelValues("flash-sale-warmup").Inc()
		} else if n > 0 {
			zlog.Info("Flash sale stock warmed/reconciled", zap.Int("activities", n))
		}
	})
	sched.Start(context.Background())
	zlog.Info("Scheduler started (idle-timeout-scan, daily/weekly task reset, event-dedup-cleanup, idle-daily-points-backfill, flash-sale-warmup)")
	return nil
}

// startHTTP 构造并启动 HTTP Server（在独立 goroutine 中 ListenAndServe，退出信号到达前持续服务）。
// 注意：IdleTimeout 必须显式设置且大于客户端心跳间隔，否则 keep-alive 连接空闲超过
// read_timeout(30s) 后会被服务端主动关闭，客户端复用该连接时得到
// "server closed idle connection"/EOF/connection reset by peer。
// ReadHeaderTimeout 单独设置以防御慢头攻击，避免与 WriteTimeout 耦合。
// MaxHeaderBytes / MaxConnsPerIP / MaxConns 为连接级防护（高并发可用性加固，需求 §6.4）：
//   - MaxHeaderBytes：限制请求头字节数，防御超大/畸形请求头占用内存与慢头攻击；
//   - MaxConnsPerIP：单 IP 并发连接数上限，防止单一客户端耗尽连接（基础连接级限流）；
//   - MaxConns：全局并发连接数上限，进程级连接耗尽防线，超过上限的新连接被立即拒绝
//     （连接级快速失败），保护后端资源不被连接风暴打垮（K8s 下仍依赖 readiness/HPA 兜底）。
func (a *App) startHTTP(handler http.Handler) (*http.Server, error) {
	cfg := a.cfg
	maxHeader := cfg.Server.MaxHeaderBytes
	if maxHeader <= 0 {
		maxHeader = 1 << 20 // 回落 net/http 默认 1MB
	}
	srv := &http.Server{
		Addr:              fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port),
		Handler:           handler,
		ReadTimeout:       cfg.Server.ReadTimeout,
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout,
		WriteTimeout:      cfg.Server.WriteTimeout,
		IdleTimeout:       cfg.Server.IdleTimeout,
		MaxHeaderBytes:    maxHeader,
	}

	// 监听套接字：这里不使用 srv.ListenAndServe()，而是自行 net.Listen 后包一层
	// connLimitListener，在 Accept 阶段实施连接级限流（全局 MaxConns / 单 IP MaxConnsPerIP）。
	// 注：Go 标准库 http.Server 并无 MaxConns/MaxConnsPerIP 字段，须由 Listener 实现。
	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", srv.Addr, err)
	}
	ln = newConnLimitListener(ln, cfg.Server)

	go func() {
		a.zlog.Info("Server listening", zap.String("addr", srv.Addr))
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			a.zlog.Error("Server failed", zap.Error(err))
		}
	}()
	return srv, nil
}

// shutdown 按序释放全部资源，每步带耗时日志，避免关闭动作藏在静默 defer 里导致退出观感变慢。
func (a *App) shutdown(srv *http.Server, bgCancel context.CancelFunc) {
	zlog := a.zlog
	shutdownStart := time.Now()
	zlog.Info("Shutting down server...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), a.cfg.Server.ShutdownTimeout)
	defer cancel()

	// 0. 优雅下线首步：停止接收新流量。就绪探针立即返回 503 → K8s 把本 Pod 从 Service 端点摘除，
	// 负载均衡不再转发新请求；随后 srv.Shutdown 仅排空已建立的在途请求（drain）。
	// 顺序早于 srv.Shutdown，确保「先摘流量、再排空中途请求」，避免关闭瞬间仍进新流量导致 5xx。
	handler.SetServing(false)

	// 每个关闭步骤带耗时日志：一旦某步变慢，日志会直接指出是"谁"慢。
	step := func(name string, fn func()) {
		t0 := time.Now()
		fn()
		zlog.Info("shutdown step done", zap.String("step", name), zap.Duration("elapsed", time.Since(t0)))
	}

	// 1. 停止接收新请求：等待 in-flight 请求完成；超时则强制关闭，但不再 Fatal 中断后续清理。
	step("http-server", func() {
		if err := srv.Shutdown(shutdownCtx); err != nil {
			zlog.Warn("HTTP graceful shutdown timed out, forcing close", zap.Error(err))
			_ = srv.Close()
		}
	})

	// 2. 取消所有后台监听/轮询协程（Kafka 订阅、死分片接管、副本发现、outbox relay）。
	step("background-goroutines", bgCancel)

	// 2b. 停止全局限流器后台清理 goroutine（cleanup ticker，10 分钟周期）。
	step("rate-limiter-cleanup", middleware.StopRateLimiter)

	// 2c. 停止验证码滑动窗口 / 白名单清理 goroutine（ticker 资源释放，避免关闭时泄漏）。
	step("captcha-cleanup", middleware.StopCaptcha)

	// 3. 停止事件驱动结算订阅（取消 Keyspace Notification 订阅协程，避免泄漏）。
	step("idle-event-driven", a.idleSvc.StopEventDriven)

	// 4. 排空并落库剩余聚合心跳（HTTP 已停，此时可安全 drain，避免丢失未落库心跳）。
	step("heartbeat-flush", func() { a.idleSvc.StopHeartbeatFlush(shutdownCtx) })

	// 5. 停止调度器并等待进行中的定时任务完成。
	step("scheduler", a.sched.Stop)

	// 6. 释放选主锁（尽快让出 Leader，加速其他副本接管）。
	if a.leader != nil {
		step("leader", a.leader.Stop)
	}

	// 7. 关闭事件消费者与生产者（生产者 Close 内置 5s 超时，broker 不可达也不拖死退出）。
	if a.eventConsumer != nil {
		step("event-consumer", func() { _ = a.eventConsumer.Close() })
	}
	if a.eventProducer != nil {
		step("event-producer", func() { _ = a.eventProducer.Close() })
	}

	// 8. 停止日志/监控异步写入器并排空缓冲（批量刷新剩余审计日志与监控指标）。
	step("log-service", a.logSvc.Close)

	// 9. 优雅关闭链路追踪（flush 残余 span 到 collector）。
	if p := trace.Global(); p != nil {
		step("tracing", func() {
			if err := p.Shutdown(shutdownCtx); err != nil {
				zlog.Warn("Trace provider shutdown error", zap.Error(err))
			}
		})
	}

	// 10. 关闭缓存（L1/L2）与全部数据库连接池（放在最后，确保上层协程已不再读写）。
	// 统一由 Databases.Close 释放 business/user/monitor/log/login 五类连接，避免各 repo 单独
	// 关闭导致的双重关闭或遗漏（此前仅关了 user/business，漏关 monitor/log/login，见 13 §3.37）。
	step("cache", a.cacheMgr.Close)
	if a.dbs != nil {
		step("databases", func() { _ = a.dbs.Close() })
	}

	// 此刻资源才真正全部释放，日志与实际退出一致。
	zlog.Info("Server exited gracefully", zap.Duration("total", time.Since(shutdownStart)))
}
