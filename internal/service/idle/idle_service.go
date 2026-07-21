package idle

import (
	"github.com/cdcdx/hc-framework-go/internal/service/common"
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/cdcdx/hc-framework-go/internal/cache"
	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/db"
	"github.com/cdcdx/hc-framework-go/internal/event"
	"github.com/cdcdx/hc-framework-go/internal/metrics"
	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/cdcdx/hc-framework-go/internal/mq"
	"github.com/cdcdx/hc-framework-go/internal/repository"
	hctrace "github.com/cdcdx/hc-framework-go/internal/trace"
)

// IdleService 挂机服务
type IdleService struct {
	cfg         *config.Config
	idleRepo    *repository.IdleRepository
	userRepo    repository.UserRepository
	shopRepo    *repository.ShopRepository
	businessDB  *db.RWDB
	logSvc      *common.LogService
	publisher   mq.Producer          // 可选：结算后发布 idle.settled 事件（未配置则为 nil）
	points      *common.PointsOutboxApplier // 积分可靠投递（Outbox），保证跨库最终一致
	cacheMgr    *cache.Manager
	eventCancel context.CancelFunc // 事件驱动结算订阅的取消函数（优雅关闭用）
	hbFlusher   *HeartbeatFlusher  // 每 Pod 本地心跳聚合器（批量落库 last_heartbeat_at，可选）

	// 离线检测扫描分片（按 Pod 分片，消除每副本全量冗余）：
	// scanShardingEnabled=true 时，scanTimeoutByRedis 仅扫描 podSetShards 内的集合分片。
	scanShardingEnabled bool
	podSetShards        []int
	shardMu             sync.RWMutex // 保护 podSetShards（死分片接管时动态变化）
	podMetaMu           sync.RWMutex // 保护 scanPodIndex/scanPodTotal（Headless 动态发现并发更新）
	scanPodIndex        int          // 本 Pod 分片序号（HC_POD_INDEX / 配置 / headless ordinal）
	scanPodTotal        int          // 分片总数（HC_POD_TOTAL / 配置）
	scanSetShards       int          // 活跃集合分片总数（= repo.ActiveSetShardCount()）
	failoverEnabled     bool         // 死分片接管开关（依赖 Leader 选举）
}

// NewIdleService 创建挂机服务（userRepo 存用户/积分，businessDB 存挂机记录）
// publisher 可选，传 nil 时不发布 Kafka 事件。
func NewIdleService(cfg *config.Config, userRepo repository.UserRepository, businessDB *db.RWDB, logSvc *common.LogService, cacheMgr *cache.Manager, publisher mq.Producer, points *common.PointsOutboxApplier) *IdleService {
	// 活跃会话集合分片数（集群分片用），<=0 退化为单集合，默认 256
	activeSetShards := cfg.Idle.ActiveSetShards
	if activeSetShards <= 0 {
		activeSetShards = 256
	}
	repo := repository.NewIdleRepositoryWithCache(businessDB, cacheMgr, activeSetShards, cfg.Idle.HeartbeatPersistEnabled)
	shopRepo := repository.NewShopRepositoryWithCache(businessDB, cacheMgr)
	svc := &IdleService{
		cfg:        cfg,
		idleRepo:   repo,
		userRepo:   userRepo,
		shopRepo:   shopRepo,
		businessDB: businessDB,
		logSvc:     logSvc,
		publisher:  publisher,
		points:     points,
		cacheMgr:   cacheMgr,
	}
	// 启用心跳批量落库：创建每 Pod 本地聚合器（由 StartHeartbeatFlush 启动定时 flush）
	if cfg.Idle.HeartbeatPersistEnabled {
		svc.hbFlusher = NewHeartbeatFlusher(repo, cfg.Idle.HeartbeatPersistIntervalOrDefault(), zap.L())
	}
	// 离线检测扫描分片：计算每个 Pod 负责的活跃集合分片序号（按 Pod 序号取模分配）。
	// 未启用时 podSetShards 为全量（退化为原有全量扫描行为）。
	if cfg.Idle.ScanSharding.Enabled {
		idx, total := ResolvePodShard(cfg.Idle.ScanSharding)
		svc.scanShardingEnabled = true
		svc.setPodIndex(idx)
		svc.setPodTotal(total)
		svc.scanSetShards = repo.ActiveSetShardCount()
		svc.failoverEnabled = cfg.Idle.ScanSharding.Failover
		svc.podSetShards = calcPodSetShards(idx, total, svc.scanSetShards)
		zap.L().Info("idle scan sharding enabled",
			zap.Int("pod_index", idx), zap.Int("pod_total", total),
			zap.String("pod_discovery", cfg.Idle.ScanSharding.PodDiscovery),
			zap.Bool("failover", svc.failoverEnabled),
			zap.Int("owned_set_shards", len(svc.podSetShards)))
	}
	return svc
}

// maxDevices 获取允许的最大同时挂机设备数，默认 1
func (s *IdleService) maxDevices() int {
	if s.cfg.Idle.MaxDevices <= 0 {
		return 1
	}
	return s.cfg.Idle.MaxDevices
}

// evictOldestIfFull 若活跃设备数已达上限，结算最早会话为新设备腾位。
// 优化：直接查询所有活跃设备并判断长度，省去 CountActive 的额外 DB 查询。
func (s *IdleService) evictOldestIfFull(ctx context.Context, userID string, maxDev int) error {
	allActive, err := s.idleRepo.FindActiveAll(ctx, userID)
	if err != nil {
		return fmt.Errorf("find active all: %w", err)
	}
	if len(allActive) < maxDev {
		return nil
	}
	if _, err := s.settleSession(ctx, &allActive[0], true); err != nil {
		return fmt.Errorf("settle oldest session: %w", err)
	}
	return nil
}

// ScanShardingEnabled 返回离线检测是否按 Pod 分片扫描（供 main 调度器判断是否需选主协调）。
func (s *IdleService) ScanShardingEnabled() bool {
	return s.scanShardingEnabled
}

// SetScanShards 动态更新本 Pod 负责的集合分片（死分片接管协调器周期性调用）。
// 用 RWMutex 保护，与 scanTimeoutByRedis 的并发读取不冲突。
func (s *IdleService) SetScanShards(shards []int) {
	s.shardMu.Lock()
	s.podSetShards = shards
	s.shardMu.Unlock()
}

// FailoverEnabled 返回死分片接管是否开启（依赖 Leader 选举）。
func (s *IdleService) FailoverEnabled() bool {
	return s.failoverEnabled
}

// ScanPodIndex / ScanPodTotal / ScanSetShards 暴露本 Pod 分片元信息，供 main 故障转移协调器使用。
func (s *IdleService) ScanPodIndex() int  { return s.podIndex() }
func (s *IdleService) ScanPodTotal() int  { return s.podTotal() }
func (s *IdleService) ScanSetShards() int { return s.scanSetShards }

// podIndex / podTotal 加锁读取本 Pod 分片元信息（Headless 动态发现会并发更新）。
func (s *IdleService) podIndex() int {
	s.podMetaMu.RLock()
	defer s.podMetaMu.RUnlock()
	return s.scanPodIndex
}
func (s *IdleService) podTotal() int {
	s.podMetaMu.RLock()
	defer s.podMetaMu.RUnlock()
	return s.scanPodTotal
}
func (s *IdleService) setPodIndex(i int) {
	s.podMetaMu.Lock()
	s.scanPodIndex = i
	s.podMetaMu.Unlock()
}
func (s *IdleService) setPodTotal(t int) {
	s.podMetaMu.Lock()
	s.scanPodTotal = t
	s.podMetaMu.Unlock()
}

// ownedSetShards 返回当前生效的（可能已含接管死分片的）集合分片，供 scanner 读取。
func (s *IdleService) ownedSetShards() []int {
	s.shardMu.RLock()
	defer s.shardMu.RUnlock()
	return s.podSetShards
}

// ownsSessionByShard 判断某会话（userID）是否由本 Pod 负责（按 Pod 分片，与离线检测 scanner 同规则）。
// 仅当扫描分片启用且 podTotal>1 时生效；否则返回 true（所有实例都处理，靠分布式锁/乐观锁幂等）。
// 用于事件驱动结算 consumer 过滤非本 Pod 的过期事件，消除多副本间的抢锁竞争与冗余 DB 回源。
func (s *IdleService) ownsSessionByShard(userID string) bool {
	if !s.scanShardingEnabled || s.podTotal() <= 1 {
		return true
	}
	return ownsSetShard(sessionSetShard(userID, s.scanSetShards), s.podIndex(), s.podTotal())
}

// StartScanFailover 启动“死分片接管”协调器（后台 goroutine）。
// 仅当扫描分片 + failover 均开启、且 L2(Redis) 可用时生效；依赖选主器提供的 isLeader 判定
// （无选主时为 nil → 各 Pod 仅扫自身分片，不接管）。
//
// 机制：每个 Pod 周期性向 Redis 写入短 TTL 存活标记 idle:pod-alive:{podIndex}；
// Leader 探测各 Pod 存活，将“曾存活但当前失活”的 Pod 的全部集合分片并入自身扫描范围，
// 消除静态分片下“Pod 宕机 → 其分片永不扫描”的可用性缺口。结算幂等（乐观锁），接管期间重复扫描安全。
//
// 冷启动防误接管：首轮仅记录已发现存活的 Pod，不立即接管其他 Pod（避免其他副本尚未注册存活标记
// 时被误判死亡而全量接管）；后续轮次仅接管“曾见存活、当前消失”的 Pod。
func (s *IdleService) StartScanFailover(ctx context.Context, isLeader func() bool) {
	if !s.scanShardingEnabled || !s.failoverEnabled {
		return
	}
	if s.cacheMgr == nil || s.cacheMgr.L2 == nil {
		zap.L().Warn("idle scan failover disabled: L2 cache unavailable, fallback to static sharding")
		return
	}
	l2 := s.cacheMgr.L2
	if s.podTotal() <= 1 {
		return
	}
	podIndex, setShards := s.podIndex(), s.scanSetShards
	livenessKey := func(i int) string { return fmt.Sprintf("idle:pod-alive:%d", i) }
	const (
		livenessTTL    = 10 * time.Second
		reconcileEvery = 5 * time.Second
	)

	seenAlive := make(map[int]bool, s.podTotal())
	reconcile := func() {
		// 动态读取 pod_total：HPA 扩缩后 Headless Service 发现会更新该值，下一轮 reconcile 即生效，
		// 使“死分片接管”范围与新副本总数一致（扩容出的新 Pod 由它自己经 DNS 感知，本 Pod 不越权接管）。
		total := s.podTotal()
		// 1. 上报本 Pod 存活
		_ = l2.Set(ctx, livenessKey(podIndex), "1", livenessTTL)
		// 2. 探测当前存活 Pod 集合
		nowAlive := make(map[int]bool, total)
		for i := 0; i < total; i++ {
			if ok, _ := l2.Exists(ctx, livenessKey(i)); ok {
				nowAlive[i] = true
			}
		}
		nowAlive[podIndex] = true

		// 3. 首轮：仅记录已发现存活，不接管
		if len(seenAlive) == 0 {
			for k := range nowAlive {
				seenAlive[k] = true
			}
			s.SetScanShards(calcPodSetShards(podIndex, total, setShards))
			return
		}
		// 4. 后续：alive = “曾见存活且当前仍存活”的 Pod；其余视为失活
		alive := make(map[int]bool, total)
		for i := 0; i < total; i++ {
			if seenAlive[i] && nowAlive[i] {
				alive[i] = true
			}
		}
		alive[podIndex] = true
		leader := isLeader != nil && isLeader()
		s.SetScanShards(ComputeOwnedShards(podIndex, total, setShards, alive, leader))
		// 5. 更新已见存活集合（逐步发现所有 Pod）
		for k := range nowAlive {
			seenAlive[k] = true
		}
		zap.L().Debug("idle scan failover reconciled",
			zap.Int("pod_index", podIndex), zap.Bool("is_leader", isLeader != nil && isLeader()),
			zap.Int("alive_pods", len(alive)), zap.Int("owned_set_shards", len(s.ownedSetShards())))
	}

	reconcile() // 立即执行首轮（仅记录，不接管）
	tick := time.NewTicker(reconcileEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			reconcile()
		}
	}
}

// StartPodDiscovery 启动 Headless Service 动态副本发现（后台 goroutine，仅 pod_discovery=headless 时生效）。
// 周期性（默认 30s）解析 Headless Service 的 DNS A 记录得到当前就绪副本数，作为 pod_total；
// 与本地配置/环境变量相互独立，使 HPA 扩缩时各 Pod 自动感知新副本总数，无需手改配置或滚动重启。
//
// 与死分片接管（failover）的协同：
//   - 扩容：新 Pod 经 DNS 自行发现更大 pod_total 并扫描自身分片；本 Pod 发现 pod_total 增大后，
//     下一轮 failover reconcile 自动按新总数重算（含 alive 集合），不会越权接管新 Pod 的分片。
//   - 缩容：被删 Pod 的分片由其“曾存活、当前失活”触发 failover 接管；本 Pod 的 pod_total 由 DNS
//     反映（被删 Pod 不再就绪 → 记录数减少），分片映射随之收敛。
//
// 未启用 failover 时，pod_total 变化直接重算本 Pod 负责的分片集合。
//
// 注：DNS 解析结果受 CoreDNS 缓存（默认 ~30s TTL）影响，扩缩后有数十秒延迟属正常；
// 期间分片映射短暂不一致由结算幂等（乐观锁）兜底，最终由下一轮扫描收敛。
func (s *IdleService) StartPodDiscovery(ctx context.Context) {
	cfg := s.cfg.Idle.ScanSharding
	if !cfg.Enabled || cfg.PodDiscovery != "headless" || cfg.HeadlessService == "" {
		return
	}
	if !s.scanShardingEnabled {
		return
	}
	svc := headlessFQDN(cfg.HeadlessService)
	resolver := defaultPodTotalResolver
	const discoverEvery = 30 * time.Second
	zap.L().Info("idle pod discovery (headless) started",
		zap.String("headless_service", svc), zap.Int("initial_pod_total", s.podTotal()))

	refresh := func() {
		n, err := resolver(ctx, svc)
		if err != nil {
			zap.L().Warn("idle pod discovery resolve failed", zap.String("service", svc), zap.Error(err))
			return
		}
		if n < 1 {
			return
		}
		if n == s.podTotal() {
			return
		}
		s.setPodTotal(n)
		zap.L().Info("idle pod discovery updated pod_total",
			zap.Int("pod_total", n), zap.Int("pod_index", s.podIndex()))
		// failover 开启时由下一轮 reconcile 按新 total 重算分片；否则这里直接重算。
		if !s.failoverEnabled {
			s.SetScanShards(calcPodSetShards(s.podIndex(), n, s.scanSetShards))
		}
	}

	refresh() // 立即执行首轮发现
	tick := time.NewTicker(discoverEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			refresh()
		}
	}
}

// Start 开始挂机（支持多设备同时挂机）
func (s *IdleService) Start(ctx context.Context, userID, deviceID string) (record *model.IdleRecord, err error) {
	// 开启链路追踪子 Span（父为 HTTP server span）
	ctx, span := hctrace.Start(ctx, "IdleService.Start",
		hctrace.WithAttributes(map[string]interface{}{
			"user_id":   userID,
			"device_id": deviceID,
		}),
	)
	defer func() {
		if err != nil {
			span.RecordError(err)
		} else {
			span.SetStatus(hctrace.StatusCodeOK, "")
		}
		span.End()
	}()

	maxDev := s.maxDevices()

	// 1. 先查同设备是否已有活跃会话 → 有则直接返回（幂等）
	existing, err := s.idleRepo.FindActiveByDevice(ctx, userID, deviceID)
	if err != nil {
		return nil, fmt.Errorf("check device active: %w", err)
	}
	if existing != nil {
		return existing, nil
	}

	// 2. 设备数已达上限 → 结算最早会话为新设备腾位
	if err := s.evictOldestIfFull(ctx, userID, maxDev); err != nil {
		return nil, err
	}

	// 3. 创建新会话
	now := time.Now()
	record = &model.IdleRecord{
		UserID:          userID,
		DeviceID:        deviceID,
		StartTime:       now,
		LastHeartbeatAt: &now,
		Status:          "active",
	}

	if err := s.idleRepo.Create(ctx, record); err != nil || record.ID == 0 {
		// INSERT ON CONFLICT DO NOTHING / INSERT IGNORE 冲突时不报错但 record.ID 保持 0，
		// 重新查询已存在的 active 会话，保证幂等 start 返回既有会话而非空记录。
		created, findErr := s.idleRepo.FindActiveByDevice(ctx, userID, deviceID)
		if findErr != nil || created == nil {
			if err == nil {
				err = fmt.Errorf("idle record id missing after create (concurrent active session?)")
			}
			return nil, fmt.Errorf("create idle record: %w", err)
		}
		record = created
	}

	// 初始化 Redis 心跳标记：判活交由 Redis TTL 决定，首跳前亦视为存活，
	// 避免离线扫描在 Start 与首次心跳之间误判该会话超时。
	_ = s.idleRepo.TouchHeartbeat(ctx, userID, deviceID, s.cfg.Idle.TimeoutThreshold)
	// 登记到活跃会话集合，供离线检测 scanner 原生遍历（无需扫 DB）
	_ = s.idleRepo.AddActiveSession(ctx, userID, deviceID)

	s.logSvc.LogDeviceOnline(ctx, common.BuildMeta(ctx, userID), deviceID)

	return record, nil
}

// Heartbeat 心跳上报（需传入 deviceID 以精确定位会话）
func (s *IdleService) Heartbeat(ctx context.Context, userID, deviceID string) error {
	// 热路径优化（百万心跳/秒场景关键）：
	// Redis 心跳标记存在即认为会话活跃，直接续期并返回，跳过 FindActiveByDevice 的 DB/缓存查询。
	// 仅当心跳标记缺失（刚启动尚未首跳 / Redis 内存驱逐）时才回退 DB 确认会话存在并补触，
	// 避免“每次心跳都查一次 DB”带来的 O(心跳数) 读库压力与 L1 缓存膨胀。
	if s.idleRepo.HeartbeatRedisEnabled() {
		if alive, _ := s.idleRepo.IsHeartbeatAlive(ctx, userID, deviceID); alive {
			if err := s.idleRepo.TouchHeartbeat(ctx, userID, deviceID, s.cfg.Idle.TimeoutThreshold); err != nil {
				return fmt.Errorf("touch heartbeat: %w", err)
			}
			s.recordHeartbeat(userID, deviceID)
			return nil
		}
	}

	// 回退路径：心跳标记缺失，查 DB 确认会话仍活跃（防止已结算会话误续期）。
	active, err := s.idleRepo.FindActiveByDevice(ctx, userID, deviceID)
	if err != nil {
		return fmt.Errorf("find active: %w", err)
	}
	if active == nil {
		return ErrNotIdle
	}

	// 心跳去 DB 化：仅写入 Redis 带 TTL 的存活标记（见 IdleRepository.TouchHeartbeat）。
	// 不写主库、不失效缓存、不广播——心跳仅表示“会话仍然在线”。
	if err := s.idleRepo.TouchHeartbeat(ctx, userID, deviceID, s.cfg.Idle.TimeoutThreshold); err != nil {
		return fmt.Errorf("touch heartbeat: %w", err)
	}
	// 聚合心跳时间到批量落库器（若启用），避免逐次 UPDATE DB last_heartbeat_at
	s.recordHeartbeat(userID, deviceID)
	return nil
}

// Stop 停止挂机并结算
func (s *IdleService) Stop(ctx context.Context, userID string) (*model.IdleRecord, error) {
	// 多设备场景：停止所有活跃会话
	allActive, err := s.idleRepo.FindActiveAll(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("find active: %w", err)
	}
	if len(allActive) == 0 {
		return nil, ErrNotIdle
	}

	var lastSettled *model.IdleRecord
	for i := range allActive {
		if _, settleErr := s.settleSession(ctx, &allActive[i], true); settleErr != nil {
			return nil, settleErr
		}
		lastSettled = &allActive[i]
	}

	return lastSettled, nil
}

// StopDevice 停止指定设备的挂机
func (s *IdleService) StopDevice(ctx context.Context, userID, deviceID string) (*model.IdleRecord, error) {
	active, err := s.idleRepo.FindActiveByDevice(ctx, userID, deviceID)
	if err != nil {
		return nil, fmt.Errorf("find active: %w", err)
	}
	if active == nil {
		return nil, ErrNotIdle
	}

	if _, err := s.settleSession(ctx, active, true); err != nil {
		return nil, err
	}

	return active, nil
}

// Status 查询挂机状态（多设备返回设备列表）
func (s *IdleService) Status(ctx context.Context, userID string) (map[string]any, error) {
	allActive, err := s.idleRepo.FindActiveAll(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("find active: %w", err)
	}

	isIdle := len(allActive) > 0
	result := map[string]any{
		"is_idle":     isIdle,
		"max_devices": s.maxDevices(),
	}

	if isIdle {
		type deviceInfo struct {
			SessionID       int64  `json:"session_id"`
			DeviceID        string `json:"device_id"`
			StartTime       string `json:"start_time"`
			DurationMinutes int    `json:"duration_minutes"`
			LastHeartbeat   string `json:"last_heartbeat,omitempty"`
		}
		devices := make([]deviceInfo, 0, len(allActive))
		// 批量获取 Redis 心跳时间戳（单次 Pipeline，消除 N 次 Redis 往返）
		deviceIDs := make([]string, len(allActive))
		for i, r := range allActive {
			deviceIDs[i] = r.DeviceID
		}
		hbMap, _ := s.idleRepo.BatchGetHeartbeatTS(ctx, userID, deviceIDs)

		for _, r := range allActive {
			di := deviceInfo{
				SessionID:       r.ID,
				DeviceID:        r.DeviceID,
				StartTime:       r.StartTime.Format(time.RFC3339),
				DurationMinutes: int(time.Since(r.StartTime).Minutes()),
			}
			if r.LastHeartbeatAt != nil {
				di.LastHeartbeat = r.LastHeartbeatAt.Format(time.RFC3339)
			}
			// 心跳已去 DB 化：优先展示 Redis 实时心跳时间（DB last_heartbeat_at 仅在启动时写入）
			if ts, ok := hbMap[r.DeviceID]; ok && ts > 0 {
				if hb := time.Unix(ts, 0); r.LastHeartbeatAt == nil || hb.After(*r.LastHeartbeatAt) {
					di.LastHeartbeat = hb.Format(time.RFC3339)
				}
			}
			devices = append(devices, di)
		}
		result["devices"] = devices
		result["active_count"] = len(allActive)
	}

	return result, nil
}

// Records 挂机历史记录
func (s *IdleService) Records(ctx context.Context, userID string, cursor int64, limit int) ([]model.IdleRecord, int64, bool, error) {
	records, err := s.idleRepo.FindRecords(ctx, userID, cursor, limit)
	if err != nil {
		return nil, 0, false, fmt.Errorf("find records: %w", err)
	}
	trimmed, nextCursor, hasMore := common.CursorPage(records, limit, func(r model.IdleRecord) int64 { return r.ID })
	return trimmed, nextCursor, hasMore, nil
}

// settleSession 结算挂机会话。
// completed=true 表示用户主动停止（completed），false 表示离线超时（timeout）。
// 返回 applied：本次结算是否实际生效（基于 status='active' 乐观锁，用于防止多实例重复结算）。
func (s *IdleService) settleSession(ctx context.Context, record *model.IdleRecord, completed bool) (bool, error) {
	now := time.Now()
	durationSeconds := int(now.Sub(record.StartTime).Seconds())
	durationMinutes := max(durationSeconds/60, 1)

	points := int64(durationMinutes) * s.cfg.Idle.PointsPerMinute

	// 每日积分上限（修复 TOCTOU）：改用「预占-确认/回退」原子封顶。
	// 原实现先事务外读 GetDailyPoints 计算剩余额度，事务提交后才累加 Redis 计数，两个并发结算会读到
	// 相同的 alreadyEarned，各自认为有剩余额度并分别写 points，导致 Redis 计数与 DB 聚合双双超限。
	// 现由 Redis Lua 脚本在单线程内原子计算「本次可授予积分」（封顶到 DailyPointsLimit），授予即累加
	// 计数；事务提交成功则保留（确认），事务失败或被乐观锁跳过则 DECRBY 回退（释放额度）。
	limit := s.cfg.Idle.DailyPointsLimit
	var granted int64
	if limit > 0 {
		g, err := s.idleRepo.AcquireDailyPoints(ctx, record.UserID, points, limit)
		if err != nil {
			return false, fmt.Errorf("acquire daily points: %w", err)
		}
		granted = g
	} else {
		granted = points
	}
	points = granted

	status := "timeout"
	if completed {
		status = "completed"
	}

	// 标记会话终态（乐观锁：仅当仍 active 时生效）
	markFn := func(repo *repository.IdleRepository, c context.Context, id int64, et time.Time, dur int, pts int64) (bool, error) {
		if completed {
			return repo.CompleteIfActive(c, id, et, dur, pts)
		}
		return repo.TimeoutIfActive(c, id, et, dur, pts)
	}

	// 积分为 0：仅关闭会话，不写积分流水
	if points == 0 {
		applied, err := markFn(s.idleRepo, ctx, record.ID, now, durationSeconds, 0)
		if err != nil {
			return false, err
		}
		record.Status = status
		record.EndTime = &now
		record.DurationSeconds = durationSeconds
		record.PointsEarned = 0
		return applied, nil
	}

	// 在 businessDB 事务中标记终态、写入积分流水与积分 outbox（同提交）
	var outboxRec *model.PointsOutbox
	applied := false
	err := s.businessDB.Transaction(ctx, func(tx *gorm.DB) error {
		idleRepo := repository.NewIdleRepositoryFromTx(tx)
		ok, mErr := markFn(idleRepo, ctx, record.ID, now, durationSeconds, points)
		if mErr != nil {
			return mErr
		}
		if !ok {
			// 已被其他实例（或重复扫描）结算，跳过积分写入与余额更新
			return nil
		}
		applied = true

		// 写积分流水到 businessDB
		shopRepo := repository.NewShopRepositoryFromTx(tx)
		txRecord := &model.PointsTransaction{
			UserID:       record.UserID,
			ChangeAmount: points,
			ChangeType:   model.ChangeTypeIdleReward,
			ReferenceID:  fmt.Sprintf("idle:%d", record.ID),
		}
		if err := shopRepo.CreateTransaction(ctx, txRecord); err != nil {
			return fmt.Errorf("create points transaction: %w", err)
		}
		// 写积分 outbox（与以上同提交）：事务提交后由 Outbox 投递器可靠更新 userDB 余额。
		rec := &model.PointsOutbox{
			UserID:    record.UserID,
			Delta:     points,
			RefType:   "idle",
			RefID:     fmt.Sprintf("%d", record.ID),
			EventID:   fmt.Sprintf("pts:idle:%d", record.ID),
			Status:    model.OutboxStatusPending,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		if s.points != nil {
			if err := s.points.AppendOutboxInTx(tx, rec); err != nil {
				return fmt.Errorf("append points outbox: %w", err)
			}
		}
		outboxRec = rec
		return nil
	})
	if err != nil {
		// 事务失败：回退已预占的每日积分额度（释放给后续结算）
		if granted > 0 {
			_ = s.idleRepo.ReleaseDailyPoints(ctx, record.UserID, granted)
		}
		return false, err
	}

	if applied {
		// 增量维护每日积分汇总：best-effort，移出关键结算事务（见 13 §3.48「非关键路径移出
		// 请求关键路径」思路）。该汇总有 BackfillDailyPoints 兜底（启动回填 + 定时 idle-daily-points-backfill），
		// 读路径优先 Redis 计数器（由 AcquireDailyPoints 的 Lua 累加），偶发失败不影响积分余额（users 表经 outbox 已保证），
		// 故不纳入事务，避免其 DB 抖动/行锁竞争拖垮核心结算（积分流水 + Outbox）引发 ctx deadline 超时回滚。
		if points > 0 {
			bctx, bcancel := context.WithTimeout(context.Background(), 3*time.Second)
			if err := s.idleRepo.UpsertDailyPoints(bctx, record.UserID, repository.LocalDayString(), points); err != nil {
				zap.L().Warn("upsert daily points summary best-effort failed (ignored, will be backfilled)",
					zap.String("user_id", record.UserID), zap.Int64("delta", points), zap.Error(err))
			}
			bcancel()
		}
		s.finalizeSettlement(ctx, record, outboxRec, points)
	} else if granted > 0 {
		// 乐观锁冲突（已被其他实例/重复扫描结算）：回退预占额度
		_ = s.idleRepo.ReleaseDailyPoints(ctx, record.UserID, granted)
	}
	s.recordSettleMetrics(completed, applied)
	record.Status = status
	record.EndTime = &now
	record.DurationSeconds = durationSeconds
	record.PointsEarned = points
	return applied, nil
}

// finalizeSettlement 事务提交后的 best-effort 后处理：投递 Outbox、更新缓存/计数器、发布事件。
// BackfillDailyPoints 批量回填当日积分汇总（根治 getDailyPointsDB 的日初全表 SUM 风暴）。
// 委托 repo 用单条 INSERT...SELECT GROUP BY 算出所有用户当日总额写入 idle_daily_points，
// 替代「每个用户首笔查询各做一次全表 SUM」的 N 次范围扫描；之后读路径均走单行 O(1)。
// 仅补「缺失行」（repo 内 ON CONFLICT DO NOTHING），不覆盖结算时增量维护的 running total。
func (s *IdleService) BackfillDailyPoints(ctx context.Context) (int64, error) {
	return s.idleRepo.BackfillDailyPoints(ctx, repository.LocalDayString())
}

func (s *IdleService) finalizeSettlement(ctx context.Context, record *model.IdleRecord, outboxRec *model.PointsOutbox, points int64) {
	if s.points != nil {
		s.points.ApplyAsync(ctx, outboxRec)
	}
	// 每日积分额度由 settleSession 经 AcquireDailyPoints 预占时通过 Lua 累加进 Redis 计数；
	// 事务提交成功即「确认」，此处无需重复累加。
	s.idleRepo.InvalidateActiveByUser(ctx, record.UserID, record.DeviceID)
	s.idleRepo.DeleteHeartbeat(ctx, record.UserID, record.DeviceID)
	s.idleRepo.RemoveActiveSession(ctx, record.UserID, record.DeviceID)
	// 事件发布脱离请求路径：kafka 模式下 publisher.Send 是同步网络写，置于响应前会将其
	// 延迟/抖动直接传导到 settle 的 p99（与 ApplyAsync 同源思路，§3.48 第3项）。
	// 事件为 best-effort 且幂等，延后发布不影响结算结果与响应延迟。
	s.emitIdleSettledAsync(record)
}

// recordSettleMetrics 按结算结果记录 Prometheus 指标。
func (s *IdleService) recordSettleMetrics(completed, applied bool) {
	if applied {
		if completed {
			metrics.IdleSettleTotal.WithLabelValues("completed").Inc()
		} else {
			metrics.IdleSettleTotal.WithLabelValues("timeout").Inc()
		}
	} else {
		metrics.IdleSettleTotal.WithLabelValues("skipped").Inc()
	}
}

// ScanAndSettleTimeout 离线检测 scanner：扫描心跳超时的活跃会话并自动结算。
// 返回本次实际结算的会话数量。由定时任务调度器周期调用。
//
// 判活优先级：
//   - Redis 可用：依据 Redis 心跳 Key（TTL=timeout_threshold）判定离线（心跳去 DB 化后，
//     DB 的 last_heartbeat_at 不再逐次更新）。见 scanTimeoutByRedis。
//   - Redis 不可用：降级为按 DB last_heartbeat_at 范围扫描（原逻辑）。见 scanTimeoutByDB。
func (s *IdleService) ScanAndSettleTimeout(ctx context.Context) (int, error) {
	if s.idleRepo.HeartbeatRedisEnabled() {
		return s.scanTimeoutByRedis(ctx)
	}
	return s.scanTimeoutByDB(ctx)
}

// scanTimeoutByRedis 依据 Redis 原生判定离线（方案 4）：遍历活跃会话集合（idle:active:set，
// O(集合大小) 的 Redis SSCAN + 每成员一次 Exists），仅对“心跳 Key 缺失/过期”的会话回源 DB 结算。
// 相比原全表扫描，DB 负载从 O(全部活跃行) 降为 O(本周期实际离线数)。
// hbBatchSize 离线检测 scanner 每批判活心跳 Key 的数量（Pipeline 批量，将 RTT 从 O(N) 降到 O(N/batch)）。
const hbBatchSize = 1000

// scanTimeoutByRedis 依据 Redis 原生判定离线（方案 4）：
// 遍历本 Pod 负责的活跃集合分片，用 Pipeline 批量判定各成员心跳 Key 存活（仅对离线成员回源 DB 结算）。
// 相比原「逐成员串行 Exists」，百万活跃会话下 RTT 从百万次降到数百次；结算阶段用有界 worker pool 并发，
// 并对单周期结算量设预算上限，避免突发离线（如网络抖动）导致 scanner 长时间阻塞。
func (s *IdleService) scanTimeoutByRedis(ctx context.Context) (int, error) {
	start := time.Now()
	defer func() {
		metrics.IdleScanDuration.Observe(time.Since(start).Seconds())
	}()

	// 按 Pod 分片扫描：仅遍历本 Pod 负责的活跃集合分片（可能已含接管的死分片），将全量 O(N) 负载分散到多 Pod。
	var members []string
	var err error
	if s.scanShardingEnabled {
		shards := s.ownedSetShards()
		if len(shards) > 0 {
			members, err = s.idleRepo.ScanActiveSessionsInShards(ctx, shards)
		} else {
			members, err = s.idleRepo.ScanActiveSessions(ctx)
		}
	} else {
		members, err = s.idleRepo.ScanActiveSessions(ctx)
	}
	if err != nil {
		return 0, fmt.Errorf("scan active sessions: %w", err)
	}
	metrics.IdleScanMembers.Set(float64(len(members)))

	// 批量判活：按批用 Pipeline 一次性判定心跳 Key 存活，仅离线成员进入结算。
	dead := s.collectDeadSessions(ctx, members)
	metrics.IdleScanDead.Set(float64(len(dead)))

	// 并发结算：有界 worker pool + 每周期预算上限，保证 scanner 始终可回收、不阻塞。
	settled := s.settleDeadSessions(ctx, dead)
	return settled, nil
}

// collectDeadSessions 按批调用 BatchHeartbeatAlive，收集心跳 Key 已过期/缺失的离线成员。
// 单批判活失败（如 Redis 抖动）仅跳过本批，下个周期重试，避免整轮扫描中断。
func (s *IdleService) collectDeadSessions(ctx context.Context, members []string) []string {
	var dead []string
	for i := 0; i < len(members); i += hbBatchSize {
		end := i + hbBatchSize
		if end > len(members) {
			end = len(members)
		}
		chunkDead, err := s.idleRepo.BatchHeartbeatAlive(ctx, members[i:end])
		if err != nil {
			zap.L().Warn("batch heartbeat alive failed, skip chunk",
				zap.Error(err), zap.Int("from", i), zap.Int("to", end))
			continue
		}
		dead = append(dead, chunkDead...)
	}
	return dead
}

// settleDeadSessions 并发结算离线成员：有界 worker pool（默认 32）消化突发离线，
// 单周期结算量超过 ScanMaxSettlePerCycle 预算时截断，余下留待下周期，确保 scanner 不长时间阻塞。
// 返回本周期实际结算的会话数（指标由 settleSession 统一按 reason 计数）。
func (s *IdleService) settleDeadSessions(ctx context.Context, dead []string) int {
	if len(dead) == 0 {
		return 0
	}
	budget := s.cfg.Idle.ScanMaxSettlePerCycle
	if budget > 0 && len(dead) > budget {
		dead = dead[:budget]
	}
	workers := s.cfg.Idle.ScanSettleWorkers
	if workers <= 0 {
		workers = 32
	}

	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	var mu sync.Mutex
	settled := 0
	now := time.Now()
	grace := s.cfg.Idle.HeartbeatInterval * 2 // 刚启动尚未首跳的宽限窗口

	for _, m := range dead {
		userID, deviceID := repository.ParseActiveMember(m)
		if userID == "" {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(u, d string) {
			defer wg.Done()
			defer func() { <-sem }()

			// 会话可能已被结算（集合与 DB 短暂不一致）：查不到则清理集合，跳过
			rec, fErr := s.idleRepo.FindActiveByDevice(ctx, u, d)
			if fErr != nil || rec == nil {
				_ = s.idleRepo.RemoveActiveSession(ctx, u, d)
				return
			}
			// 会话刚启动、首跳尚未到达：给予宽限，避免误结算
			if grace > 0 && now.Sub(rec.StartTime) <= grace {
				return
			}
			applied, sErr := s.settleSession(ctx, rec, false)
			if sErr != nil {
				return
			}
			if !applied {
				// 已被其他实例结算（乐观锁保护），跳过
				return
			}
			mu.Lock()
			settled++
			mu.Unlock()
			s.logSvc.LogDeviceOffline(ctx, common.BuildMetaFromRequest(rec.UserID, "", ""), rec.DeviceID)
		}(userID, deviceID)
	}
	wg.Wait()
	return settled
}

// BackfillActiveSessions 启动时将 DB 中既有 active 会话回填进 Redis 活跃集合，
// 使离线检测 scanner 在热更新/重启后能继续覆盖这些会话（新会话由 Start 自行登记）。
func (s *IdleService) BackfillActiveSessions(ctx context.Context) error {
	if !s.idleRepo.HeartbeatRedisEnabled() {
		return nil
	}
	actives, err := s.idleRepo.FindAllActive(ctx)
	if err != nil {
		return fmt.Errorf("backfill active: %w", err)
	}
	for i := range actives {
		_ = s.idleRepo.AddActiveSession(ctx, actives[i].UserID, actives[i].DeviceID)
	}
	return nil
}

// ────────── 事件驱动结算（Keyspace Notification） ──────────
// 心跳 key（idle:hb:{userID}:{deviceID}，TTL=timeout_threshold）过期即触发结算，零轮询、近实时。
// 与轮询 scanner 互为补充：scanner 作为兜底（Keyspace Notification 在网络抖动/Redis 重启时可能丢事件），
// 两者都走同一幂等结算逻辑（settleSession 乐观锁），互不冲突。

// StartEventDriven 启动 Keyspace Notification 事件驱动结算订阅。
// 前置：Redis 可用且配置 event_driven_settle=true；集群模式（跨节点订阅不可靠）自动禁用并保留轮询 scanner。
func (s *IdleService) StartEventDriven(ctx context.Context) {
	if !s.idleRepo.HeartbeatRedisEnabled() {
		return
	}
	if !s.cfg.Idle.EventDrivenSettle {
		return
	}
	// 集群模式下跨节点订阅 Keyspace Notification 不可靠（每个 master 只通知自身 key），自动禁用
	if s.cfg.Cache.L2.Cluster.Enabled {
		zap.L().Warn("idle event-driven settle disabled: Redis Cluster mode (cross-node keyspace notification unreliable), fallback to polling scanner")
		return
	}

	// best-effort 开启 Ex 通知：Redis 重启后会重置；云平台 Redis 可能禁止 CONFIG 命令（需服务端预开启）
	if err := s.cacheMgr.L2.ConfigSet(ctx, "notify-keyspace-events", "Ex"); err != nil {
		zap.L().Warn("idle event-driven settle: enable notify-keyspace-events failed (ensure it is set on Redis server)", zap.Error(err))
	}

	dbNum := s.cfg.Cache.L2.DB
	channel := fmt.Sprintf("__keyevent@%d__:expired", dbNum)
	subCtx, cancel := context.WithCancel(ctx)
	s.eventCancel = cancel
	go func() {
		zap.L().Info("idle event-driven settle subscribed", zap.String("channel", channel))
		_ = s.cacheMgr.L2.Subscribe(subCtx, channel, s.handleHBExpired)
	}()
}

// handleHBExpired 处理心跳 key 过期事件（expired 频道 payload 为完整 key 名）。
func (s *IdleService) handleHBExpired(msg string) {
	userID, deviceID, ok := repository.ParseHeartbeatKey(msg)
	if !ok {
		return
	}

	// 按 Pod 分片过滤：仅处理归属本 Pod 的会话，消除多副本抢锁竞争与冗余回源。
	// 非本 Pod 分片的过期事件由对应 Pod 的 consumer 处理（与离线检测 scanner 同分片规则）。
	// 注意：静态分片下若某 Pod 宕机，其分片事件既无 consumer 也无 scanner 覆盖（与 scanner 一致）；
	// 生产建议配合 Pod 自愈或基于领导租约的“死分片接管”，此处与 scanner 保持语义一致。
	if !s.ownsSessionByShard(userID) {
		return
	}

	ctx := context.Background()

	// 多实例均会收到事件：加分布式锁，仅一个实例执行结算，避免重复回源 DB
	lockKey := fmt.Sprintf("idle:settle-lock:%s:%s", userID, deviceID)
	locked, err := s.cacheMgr.Lock(ctx, lockKey, 15*time.Second)
	if err != nil || !locked {
		return
	}
	defer s.cacheMgr.Unlock(ctx, lockKey)

	rec, fErr := s.idleRepo.FindActiveByDevice(ctx, userID, deviceID)
	if fErr != nil || rec == nil {
		// 会话已结算/不存在：清理可能残留的心跳 key 与集合成员，避免悬挂
		_ = s.idleRepo.DeleteHeartbeat(ctx, userID, deviceID)
		_ = s.idleRepo.RemoveActiveSession(ctx, userID, deviceID)
		return
	}

	// 宽限：刚启动、首跳尚未到达的会话（心跳 key 因内存逐出等非过期原因消失）暂不结算
	grace := s.cfg.Idle.HeartbeatInterval * 2
	if grace > 0 && time.Since(rec.StartTime) <= grace {
		// 重新触摸心跳 key，避免立即再次触发过期事件
		_ = s.idleRepo.TouchHeartbeat(ctx, userID, deviceID, s.cfg.Idle.TimeoutThreshold)
		return
	}

	applied, sErr := s.settleSession(ctx, rec, false)
	if sErr != nil {
		return
	}
	if !applied {
		return // 已被其他实例/路径结算（乐观锁保护）
	}
	s.logSvc.LogDeviceOffline(ctx, common.BuildMetaFromRequest(rec.UserID, "", ""), rec.DeviceID)
}

// StopEventDriven 停止事件驱动结算订阅（优雅关闭时由 main 调用）
func (s *IdleService) StopEventDriven() {
	if s.eventCancel != nil {
		s.eventCancel()
		s.eventCancel = nil
	}
}

// recordHeartbeat 聚合一次心跳时间到批量落库器（若启用了 HeartbeatPersistEnabled）。
// 仅更新本地内存 map（O(1)），无 DB 写；由 HeartbeatFlusher 定时批量 flush。
func (s *IdleService) recordHeartbeat(userID, deviceID string) {
	if s.hbFlusher != nil {
		s.hbFlusher.Record(userID, deviceID, time.Now())
	}
}

// StartHeartbeatFlush 启动心跳批量落库定时器（仅当启用了批量持久化；否则为 no-op）。
func (s *IdleService) StartHeartbeatFlush(ctx context.Context) {
	if s.hbFlusher != nil {
		s.hbFlusher.Start(ctx)
	}
}

// StopHeartbeatFlush 停止定时器并排空剩余聚合心跳（优雅关闭时调用，避免丢失未落库心跳）。
func (s *IdleService) StopHeartbeatFlush(ctx context.Context) {
	if s.hbFlusher != nil {
		s.hbFlusher.Stop(ctx)
	}
}

// FlushHeartbeats 立即批量落库（测试或关闭前手动触发）。未启用时返回 0。
func (s *IdleService) FlushHeartbeats(ctx context.Context) (int, error) {
	if s.hbFlusher == nil {
		return 0, nil
	}
	return s.hbFlusher.Flush(ctx)
}

// scanTimeoutByDB 降级路径：按 DB last_heartbeat_at 范围扫描超时会话（原实现）。
func (s *IdleService) scanTimeoutByDB(ctx context.Context) (int, error) {
	before := time.Now().Add(-s.cfg.Idle.TimeoutThreshold)
	stale, err := s.idleRepo.FindStaleActive(ctx, before)
	if err != nil {
		return 0, fmt.Errorf("scan stale active: %w", err)
	}

	settled := 0
	for i := range stale {
		rec := &stale[i]
		applied, sErr := s.settleSession(ctx, rec, false)
		if sErr != nil {
			// 单条失败不影响其余会话，记录后继续
			continue
		}
		if !applied {
			// 已被其他实例结算（乐观锁保护），跳过
			continue
		}
		settled++

		// 记录离线日志（事件发布已在 settleSession 内统一处理）
		s.logSvc.LogDeviceOffline(ctx, common.BuildMetaFromRequest(rec.UserID, "", ""), rec.DeviceID)
	}
	return settled, nil
}

// emitIdleSettled 发布挂机结算事件（仅当配置了 producer）
func (s *IdleService) emitIdleSettled(ctx context.Context, rec *model.IdleRecord) {
	if s.publisher == nil {
		return
	}
	msg := &event.Message{
		EventType: event.EventIdleSettled,
		Key:       rec.UserID,
		EventID:   fmt.Sprintf("idle:%d", rec.ID), // 幂等键：以挂机记录主键派生，天然唯一
		Timestamp: time.Now(),
		Payload: event.IdleSettledPayload{
			UserID:          rec.UserID,
			IdleRecordID:    rec.ID,
			DurationSeconds: rec.DurationSeconds,
			PointsEarned:    rec.PointsEarned,
		},
	}
	if err := s.publisher.Send(ctx, msg); err != nil {
		// 事件发布失败不影响结算结果，仅记录
		_ = err
	}
}

// emitIdleSettledAsync 脱离请求 ctx 在后台发布 idle.settled 事件，避免同步 broker 写阻塞 settle 响应。
// 使用 context.Background() 派生的「带超时」ctx，避免请求在发布完成前已返回并取消其 ctx
// （沿用会导致 Send 中途被取消、连接异常归还）。事件为 best-effort 且幂等，发布失败/延后
// 不影响结算一致性与响应延迟（与 ApplyAsync 同源思路，§3.48 第3项）。
func (s *IdleService) emitIdleSettledAsync(rec *model.IdleRecord) {
	if s.publisher == nil {
		return
	}
	dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	go func() {
		defer cancel() // cancel 必须在 goroutine 内（emitIdleSettled 完成后）执行，
		// 否则此处 defer 会在启动 goroutine 后立即返回时已取消 dctx，使 5s 超时形同虚设：
		// 对 RabbitMQ 等会读 ctx 的 Producer，Send 确认等待将立即命中 Done() 而丢事件（见 §3.64）
		s.emitIdleSettled(dctx, rec)
	}()
}

// ErrNotIdle ====================
// 服务层自定义错误
var ErrNotIdle = fmt.Errorf("not in idle state")
var ErrMaxDevices = fmt.Errorf("maximum active devices reached")

// EnsureIndexes 确保挂机相关的数据库索引（启动时调用一次）
func (s *IdleService) EnsureIndexes() error {
	return s.idleRepo.EnsureUniqueIndex()
}
