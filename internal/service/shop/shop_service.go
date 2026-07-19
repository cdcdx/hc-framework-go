package shop

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/cdcdx/hc-framework-go/internal/service/common"
	hctrace "github.com/cdcdx/hc-framework-go/internal/trace"
)

// ShopService 商城服务
type ShopService struct {
	cfg        *config.Config
	shopRepo   *repository.ShopRepository
	userRepo   repository.UserRepository
	businessDB *db.RWDB
	logSvc     *common.LogService
	cacheMgr   *cache.Manager              // 可为 nil（未启用缓存时）
	publisher  mq.Producer                 // 可选：兑换后发布 shop.redeemed 事件（未配置则为 nil）
	points     *common.PointsOutboxApplier // 积分可靠投递（Outbox），保证跨库最终一致
}

// NewShopService 创建商城服务（userRepo 存积分余额，businessDB 存商品/订单）
// publisher 可选，传 nil 时不发布 Kafka 事件。
func NewShopService(cfg *config.Config, userRepo repository.UserRepository, businessDB *db.RWDB, logSvc *common.LogService, cacheMgr *cache.Manager, publisher mq.Producer, points *common.PointsOutboxApplier) *ShopService {
	return &ShopService{
		cfg:        cfg,
		shopRepo:   repository.NewShopRepositoryWithCache(businessDB, cacheMgr),
		userRepo:   userRepo,
		businessDB: businessDB,
		logSvc:     logSvc,
		cacheMgr:   cacheMgr,
		publisher:  publisher,
		points:     points,
	}
}

// Items 商品列表（游标分页）
func (s *ShopService) Items(ctx context.Context, cursor int64, limit int, category string) ([]model.ShopItem, int64, bool, error) {
	items, err := s.shopRepo.FindItems(ctx, cursor, limit, category)
	if err != nil {
		return nil, 0, false, fmt.Errorf("find items: %w", err)
	}
	trimmed, nextCursor, hasMore := common.CursorPage(items, limit, func(it model.ShopItem) int64 { return it.ID })
	for i := range trimmed {
		trimmed[i].SalesStatus = trimmed[i].SalesStatusValue()
	}
	return trimmed, nextCursor, hasMore, nil
}

// RedeemRequest 兑换请求
type RedeemRequest struct {
	ItemID   int64 `json:"item_id"`
	Quantity int   `json:"quantity"`
}

// Redeem 积分兑换
func (s *ShopService) Redeem(ctx context.Context, userID string, req *RedeemRequest) (order *model.RedeemOrder, err error) {
	if req.Quantity <= 0 {
		req.Quantity = 1
	}

	// 开启链路追踪子 Span（父为 HTTP server span，ctx 已携带）
	ctx, span := hctrace.Start(ctx, "ShopService.Redeem",
		hctrace.WithAttributes(map[string]interface{}{
			"user_id":  userID,
			"item_id":  req.ItemID,
			"quantity": req.Quantity,
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

	// 普通兑换结果计数（可观测性加固，P1/P2）：区分削峰层售罄(sold_out_peak) 与 DB 行锁
	// 售罄(sold_out_db)、并发冲突(concurrent)，直接观测「削峰拦截占比」与容量瓶颈。
	defer func() {
		metrics.RedeemTotal.WithLabelValues(redeemResultLabel(err)).Inc()
	}()

	// 同一用户的并发兑换用分布式锁串行化，避免超卖与重复扣减。
	// 两种失败情形不同，分别处理：
	//   - err != nil：Redis 异常，锁后端不可用 → 直接阻断（此时乐观锁兜底已不可靠，不能冒险放行）；
	//   - locked==false：锁被其他并发兑换持有 → 返回明确的并发冲突错误让客户端重试。
	lockKey := fmt.Sprintf("redeem:lock:%s", userID)
	lockTTL := 10 * time.Second
	locked, err := s.acquireLock(ctx, lockKey, lockTTL)
	if err != nil {
		return nil, fmt.Errorf("acquire redeem lock: %w", err)
	}
	if !locked {
		return nil, ErrRedeemConcurrent
	}
	defer s.releaseLock(ctx, lockKey)

	// 校验商品。DeductStock 已改为原子条件扣减（WHERE stock>=quantity），不依赖 version
	// 乐观锁，因此此处走默认读路径（从库 + 三级缓存）即可，无需强读主库。真正的库存扣减
	// 与防超卖由 DB 行锁兜底，缓存中 stock/version 略滞后无害。
	item, err := s.shopRepo.FindItemByID(ctx, req.ItemID)
	if err != nil {
		return nil, fmt.Errorf("find item: %w", err)
	}
	if item == nil || !item.IsActive {
		return nil, ErrItemNotFound
	}
	if item.Stock < req.Quantity {
		return nil, ErrStockInsufficient
	}

	// Redis 原子预扣（削峰）：已售罄请求在此快速拒绝，不进入后续用户查询/DB 事务，
	// 避免海量无效请求打到 DB 行锁与连接池（shop-flash 压测中 99.97% 为库存不足的空刀风暴）。
	// 预扣成功后若最终未提交（售罄兜底/积分不足/事务失败等）需回滚预扣（acquiredRedis + defer）。
	// DB 条件更新（WHERE stock>=quantity）仍是权威防超卖，Redis 仅作前置拦截层，零超卖不受影响。
	//
	// 削峰层售罄用独立错误 ErrRedeemSoldOutPeak（业务码 10311），与 DB 行锁售罄 ErrStockInsufficient
	// （10302）区分，使普通兑换也具备与抢购一致的「削峰拦截占比」可观测性（P1-4 修复）。
	acquiredRedis := false
	committed := false
	if s.cacheMgr != nil && s.cacheMgr.L2Enabled() {
		res, _, aerr := s.cacheMgr.TryAcquireRedeem(ctx, item.ID, int(item.Stock), redeemStockTTL)
		// Redis 异常时降级：不阻断，交由 DB 权威兜底。
		if aerr == nil {
			switch res {
			case cache.RedeemSoldOut:
				return nil, ErrRedeemSoldOutPeak
			case cache.RedeemOK:
				acquiredRedis = true
			}
		}
	}
	defer func() {
		if acquiredRedis && !committed {
			_ = s.cacheMgr.ReleaseRedeem(ctx, item.ID)
		}
	}()

	totalPoints := item.PricePoints * int64(req.Quantity)

	// 校验积分
	user, err := s.userRepo.FindByID(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("find user: %w", err)
	}
	if user == nil {
		return nil, ErrUserNotFound
	}
	if user.PointsBalance < totalPoints {
		return nil, ErrPointsInsufficient
	}

	// 在 businessDB 事务中完成兑换：扣库存 + 建订单 + 写流水 + 写积分 outbox（同提交）
	var outboxRec *model.PointsOutbox
	err = s.businessDB.Transaction(ctx, func(tx *gorm.DB) error {
		shopRepo := repository.NewShopRepositoryFromTx(tx)

		// 原子条件扣减库存（分桶路径：WHERE stock>=quantity 作用于桶行，行锁防超卖；
		// 无桶商品回退单行语义）。shardKey 传 userID，使同人稳定命中同桶、不同人分散到不同桶。
		if err := shopRepo.DeductStock(ctx, item.ID, req.Quantity, userID); err != nil {
			return ErrStockInsufficient
		}

		// 创建订单
		order = &model.RedeemOrder{
			UserID:      userID,
			ItemID:      item.ID,
			ItemName:    item.Name,
			PointsSpent: totalPoints,
			OrderStatus: "completed",
		}
		if err := shopRepo.CreateOrder(ctx, order); err != nil {
			return fmt.Errorf("create order: %w", err)
		}

		// 写积分流水
		txRecord := &model.PointsTransaction{
			UserID:       userID,
			ChangeAmount: -totalPoints,
			BalanceAfter: user.PointsBalance - totalPoints,
			ChangeType:   model.ChangeTypeRedeemSpend,
			ReferenceID:  fmt.Sprintf("order:%d", order.ID),
		}
		if err := shopRepo.CreateTransaction(ctx, txRecord); err != nil {
			return fmt.Errorf("create points transaction: %w", err)
		}

		// 写积分 outbox（与以上同提交）：事务提交后由 Outbox 投递器可靠更新 userDB 余额。
		rec := &model.PointsOutbox{
			UserID:    userID,
			Delta:     -totalPoints,
			RefType:   "redeem",
			RefID:     fmt.Sprintf("%d", order.ID),
			EventID:   fmt.Sprintf("pts:redeem:%d", order.ID),
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
		return nil, err
	}
	committed = true

	// 跨库一致：通过 Outbox 可靠投递积分调整（businessDB 已提交 outbox 记录），
	// 同步快路径 + Kafka 消费者 + 后台 relay 竞争应用；userDB 失败也不影响本次兑换结果。
	if s.points != nil {
		s.points.ApplyAsync(ctx, outboxRec)
	}

	// 写商城兑换日志 + 监控指标
	s.logSvc.LogShopRedeem(ctx, common.BuildMeta(ctx, userID), item.ID, item.Name, totalPoints)

	// 发布 shop.redeemed 事件（事件驱动兑换任务进度；未配置 producer 时静默跳过）
	s.emitShopRedeemed(ctx, userID, order, item, totalPoints)

	return order, nil
}

// FlashActivities 查询进行中的抢购活动列表（用户视角，只读进行中活动）。
// 列表经三级缓存（FlashActivityListCacheKey，默认 5s TTL）削峰：开抢瞬间大量用户轮询列表时，
// 仅首请求回源 DB，后续命中缓存，DB 读取量下降约两个数量级（高并发保护）。
func (s *ShopService) FlashActivities(ctx context.Context, limit int) ([]model.ShopFlashActivity, error) {
	load := func(c context.Context) (interface{}, error) {
		return s.shopRepo.FindActivities(c, false, limit)
	}
	if s.cacheMgr != nil && s.cacheMgr.L2Enabled() {
		if val, err := s.cacheMgr.Get(ctx, cache.FlashActivityListCacheKey(), flashActivityMetaTTL, load); err == nil {
			if acts, e := cache.DecodeCached[[]model.ShopFlashActivity](val); e == nil {
				metrics.FlashSaleActiveActivities.Set(float64(len(acts)))
				return acts, nil
			}
		}
	}
	acts, err := s.shopRepo.FindActivities(ctx, false, limit)
	if err != nil {
		return nil, fmt.Errorf("find active activities: %w", err)
	}
	for i := range acts {
		acts[i].SalesStatus = acts[i].SalesStatusValue()
	}
	metrics.FlashSaleActiveActivities.Set(float64(len(acts)))
	return acts, nil
}

// ListFlashActivities 查询抢购活动列表（运营视角，含已结束，全状态）。
func (s *ShopService) ListFlashActivities(ctx context.Context, limit int) ([]model.ShopFlashActivity, error) {
	acts, err := s.shopRepo.FindActivities(ctx, true, limit)
	if err != nil {
		return nil, fmt.Errorf("find activities: %w", err)
	}
	for i := range acts {
		acts[i].SalesStatus = acts[i].SalesStatusValue()
	}
	return acts, nil
}

// FlashActivityDetail 查询单个抢购活动详情（运营视角）。
func (s *ShopService) FlashActivityDetail(ctx context.Context, id int64) (*model.ShopFlashActivity, error) {
	act, err := s.getFlashActivity(ctx, id)
	if err != nil {
		return nil, err
	}
	if act == nil {
		return nil, ErrFlashSaleNotFound
	}
	act.SalesStatus = act.SalesStatusValue()
	return act, nil
}

// FlashActivityInput 创建/更新抢购活动的入参。
type FlashActivityInput struct {
	ItemID       int64      `json:"item_id"`
	Name         string     `json:"name"`
	StartTime    time.Time  `json:"start_time"`
	EndTime      *time.Time `json:"end_time"` // nil 表示不限结束时间
	LimitQty     int        `json:"limit_qty"`
	PerUserLimit int        `json:"per_user_limit"`
	PricePoints  int64      `json:"price_points"`
	Status       string     `json:"status"` // 可选，缺省按开始时间推导（未来= pending，过去/现在= active）
}

// CreateFlashActivity 创建抢购活动（运营接口）。
// 创建成功后立即预热 Redis 库存并写入元信息缓存（best-effort，L2 不可用时降级为纯 DB 兜底），
// 使活动一开放即可被削峰层快速拦截，且用户侧列表/详情立即可见。
func (s *ShopService) CreateFlashActivity(ctx context.Context, in *FlashActivityInput) (*model.ShopFlashActivity, error) {
	if in == nil || in.ItemID <= 0 {
		return nil, fmt.Errorf("item_id is required")
	}
	if in.LimitQty <= 0 {
		return nil, fmt.Errorf("limit_qty must be > 0")
	}
	perUser := in.PerUserLimit
	if perUser <= 0 {
		perUser = 1
	}
	status := in.Status
	if status == "" {
		if in.StartTime.After(time.Now()) {
			status = model.FlashSaleStatusPending
		} else {
			status = model.FlashSaleStatusActive
		}
	}
	act := &model.ShopFlashActivity{
		ItemID:       in.ItemID,
		Name:         in.Name,
		StartTime:    in.StartTime,
		EndTime:      in.EndTime,
		LimitQty:     in.LimitQty,
		SoldQty:      0,
		PerUserLimit: perUser,
		PricePoints:  in.PricePoints,
		Status:       status,
	}
	if err := s.shopRepo.CreateActivity(ctx, act); err != nil {
		return nil, fmt.Errorf("create activity: %w", err)
	}
	act.SalesStatus = act.SalesStatusValue()
	// 预热 Redis 库存（剩余 = 上限，开抢前预填）并写元信息缓存，best-effort。
	if s.cacheMgr != nil {
		_ = s.cacheMgr.Set(ctx, cache.FlashActivityCacheKey(act.ID), act, flashActivityMetaTTL)
		_ = s.reconcileStock(ctx, act)
	}
	return act, nil
}

// UpdateFlashActivity 更新抢购活动（运营接口，状态/限量/时间窗/每人限购等）。
// 不修改运行期权威计数 sold_qty；更新后失效相关缓存。
func (s *ShopService) UpdateFlashActivity(ctx context.Context, id int64, in *FlashActivityInput) (*model.ShopFlashActivity, error) {
	act, err := s.shopRepo.FindActivityByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("find activity: %w", err)
	}
	if act == nil {
		return nil, ErrFlashSaleNotFound
	}
	if in.Name != "" {
		act.Name = in.Name
	}
	if !in.StartTime.IsZero() {
		act.StartTime = in.StartTime
	}
	act.EndTime = in.EndTime // 允许清空（零值=不限结束）
	if in.LimitQty > 0 {
		act.LimitQty = in.LimitQty
	}
	if in.PerUserLimit > 0 {
		act.PerUserLimit = in.PerUserLimit
	}
	if in.PricePoints >= 0 {
		act.PricePoints = in.PricePoints
	}
	if in.Status != "" {
		act.Status = in.Status
	}
	if err := s.shopRepo.UpdateActivity(ctx, act); err != nil {
		return nil, fmt.Errorf("update activity: %w", err)
	}
	// 失效缓存：元信息 + 列表（下次读取回源最新值）。
	if s.cacheMgr != nil {
		_ = s.cacheMgr.Delete(ctx, cache.FlashActivityCacheKey(id), cache.FlashActivityListCacheKey())
		_ = s.reconcileStock(ctx, act) // 限量上调后同步抬高 Redis 库存上限
	}
	return act, nil
}

// EndFlashActivity 下架/结束抢购活动（运营接口）：置状态为 ended。
func (s *ShopService) EndFlashActivity(ctx context.Context, id int64) (*model.ShopFlashActivity, error) {
	act, err := s.shopRepo.FindActivityByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("find activity: %w", err)
	}
	if act == nil {
		return nil, ErrFlashSaleNotFound
	}
	act.Status = model.FlashSaleStatusEnded
	if err := s.shopRepo.UpdateActivity(ctx, act); err != nil {
		return nil, fmt.Errorf("end activity: %w", err)
	}
	if s.cacheMgr != nil {
		_ = s.cacheMgr.Delete(ctx, cache.FlashActivityCacheKey(id), cache.FlashActivityListCacheKey())
	}
	return act, nil
}

// WarmupFlashSale 手动预热/对齐某活动的 Redis 库存（运营接口）：
// 以 DB 权威剩余值（limit_qty - sold_qty）强制重置 Redis 计数器，消除漂移。
func (s *ShopService) WarmupFlashSale(ctx context.Context, id int64) error {
	act, err := s.shopRepo.FindActivityByID(ctx, id)
	if err != nil {
		return fmt.Errorf("find activity: %w", err)
	}
	if act == nil {
		return ErrFlashSaleNotFound
	}
	return s.reconcileStock(ctx, act)
}

// SyncFlashSaleStock 库存对账（运营接口，语义同 WarmupFlashSale，强调事故后修复）：
// 以 DB 权威值重置 Redis 库存。L2 不可用时返回 nil（纯 DB 兜底无需对账）。
func (s *ShopService) SyncFlashSaleStock(ctx context.Context, id int64) error {
	return s.WarmupFlashSale(ctx, id)
}

// WarmupDueFlashSales 调度器周期任务：为「即将开抢 / 已开始」的活动预热并对齐 Redis 库存。
// 仅 Leader 执行（bootstrap 已做选主门控），多副本部署不会重复写；幂等（Reconcile 强制覆盖）。
// 返回成功对账的活动数。
func (s *ShopService) WarmupDueFlashSales(ctx context.Context) (int, error) {
	now := time.Now()
	// 周期校正活动生命周期状态：到点的 pending→active（预售活动开抢后出现在用户列表），
	// 过期的 active→ended。幂等（已是目标状态不受影响），与 Redis 是否启用无关。
	if shifted, terr := s.shopRepo.TransitionStaleStatuses(ctx, now); terr != nil {
		zap.L().Warn("flash sale status transition failed", zap.Error(terr))
	} else if shifted > 0 {
		// 状态变化后失效列表缓存，使用户侧列表/详情在下一读即刷新为最新生命周期。
		if s.cacheMgr != nil {
			_ = s.cacheMgr.Delete(ctx, cache.FlashActivityListCacheKey())
		}
	}
	acts, err := s.shopRepo.ListWarmupCandidates(ctx, now, flashWarmupLead)
	if err != nil {
		return 0, fmt.Errorf("list warmup candidates: %w", err)
	}
	if s.cacheMgr == nil {
		return 0, nil
	}
	n := 0
	for i := range acts {
		if err := s.reconcileStock(ctx, &acts[i]); err != nil {
			zap.L().Warn("flash sale warmup/reconcile failed", zap.Int64("activity_id", acts[i].ID), zap.Error(err))
			metrics.FlashSaleWarmupTotal.WithLabelValues("error").Inc()
			continue
		}
		n++
		metrics.FlashSaleWarmupTotal.WithLabelValues("warmed").Inc()
	}
	return n, nil
}

// reconcileStock 以 DB 权威剩余值（limit_qty - sold_qty）强制重置某活动的 Redis 库存计数器。
// L2 不可用时为 no-op（纯 DB 兜底，无需 Redis 对账）。best-effort，错误向上返回供调用方计数。
func (s *ShopService) reconcileStock(ctx context.Context, act *model.ShopFlashActivity) error {
	if s.cacheMgr == nil || !s.cacheMgr.L2Enabled() {
		metrics.FlashSaleWarmupTotal.WithLabelValues("skipped").Inc()
		return nil
	}
	remaining := int64(act.LimitQty - act.SoldQty)
	if err := s.cacheMgr.ReconcileFlashSale(ctx, act.ID, remaining, flashSaleTTL(act)); err != nil {
		return err
	}
	return nil
}

// getFlashActivity 读取抢购活动，优先三级缓存（FlashActivityCacheKey，默认 5s TTL）削峰，
// 缓存未命中回源 DB。未找到返回 (nil, nil)。高并发开抢时，活动元信息读取被缓存大幅削减。
func (s *ShopService) getFlashActivity(ctx context.Context, id int64) (*model.ShopFlashActivity, error) {
	load := func(c context.Context) (interface{}, error) {
		return s.shopRepo.FindActivityByID(c, id)
	}
	if s.cacheMgr != nil && s.cacheMgr.L2Enabled() {
		if val, err := s.cacheMgr.Get(ctx, cache.FlashActivityCacheKey(id), flashActivityMetaTTL, load); err == nil {
			if act, e := cache.DecodeCached[*model.ShopFlashActivity](val); e == nil && act != nil && act.ID != 0 {
				return act, nil
			}
		}
	}
	return s.shopRepo.FindActivityByID(ctx, id)
}

// FlashRedeem 定时抢购兑换：在活动时间窗内、全场限量内先到先得。
//
// 「限定个数内入库兑换，限定个数外直接返回不入库」的实现：
//  1. Redis 原子预扣（削峰快速拦截层，可选）：超出限量/每人限购者在此直接拒绝，不触达 DB；
//  2. DB 条件更新兜底（权威）：AcquireFlashQuota 仅当 sold_qty<limit_qty 时 +1，
//     RowsAffected==0（已抢光）在事务内即返回错误回滚，绝不创建订单/扣积分 → 不入库。
//
// 只有两道防线都通过，才在同一事务内创建订单、写积分流水与 Outbox（入库兑换）。
func (s *ShopService) FlashRedeem(ctx context.Context, userID string, activityID int64) (order *model.RedeemOrder, err error) {
	ctx, span := hctrace.Start(ctx, "ShopService.FlashRedeem",
		hctrace.WithAttributes(map[string]interface{}{
			"user_id":     userID,
			"activity_id": activityID,
		}),
	)
	defer func() {
		metrics.FlashSaleRedeemTotal.WithLabelValues(flashResultLabel(err)).Inc()
		// 独立容量告警指标：写事务 deadline 超时（context.DeadlineExceeded）单独累加，
		// 不被 other 标签淹没，便于配置「尖峰期超时」独立告警（对应业务码 10309）。
		if err != nil && errors.Is(err, context.DeadlineExceeded) {
			metrics.FlashSaleRedeemTimeoutTotal.Inc()
		}
		if err != nil {
			span.RecordError(err)
		} else {
			span.SetStatus(hctrace.StatusCodeOK, "")
		}
		span.End()
	}()

	// 1. 校验活动与时间窗（活动元信息走三级缓存削峰）
	act, err := s.getFlashActivity(ctx, activityID)
	if err != nil {
		return nil, fmt.Errorf("find activity: %w", err)
	}
	if act == nil || act.Status == model.FlashSaleStatusEnded {
		return nil, ErrFlashSaleNotFound
	}
	now := time.Now()
	if act.Status == model.FlashSaleStatusPending || now.Before(act.StartTime) {
		return nil, ErrFlashSaleNotStarted
	}
	if act.EndTime != nil && now.After(*act.EndTime) {
		return nil, ErrFlashSaleEnded
	}
	perUserLimit := act.PerUserLimit
	if perUserLimit <= 0 {
		perUserLimit = 1
	}

	// 2. 同一用户对同一活动的并发抢购用分布式锁串行化（防一人并发多抢绕过每人限购）。
	lockKey := fmt.Sprintf("redeem:flash:lock:%s:%d", userID, activityID)
	locked, err := s.acquireLock(ctx, lockKey, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("acquire flash lock: %w", err)
	}
	if !locked {
		return nil, ErrRedeemConcurrent
	}
	defer s.releaseLock(ctx, lockKey)

	// 3. Redis 原子预扣（削峰）：超出限量/每人限购在此快速拒绝，不入库、不触达 DB。
	//    预扣成功后若后续任一步失败，需回滚预扣（acquiredRedis 标志 + defer 兜底）。
	acquiredRedis := false
	committed := false
	if s.cacheMgr != nil {
		res, _, aerr := s.cacheMgr.TryAcquireFlashSale(ctx, activityID, userID, act.LimitQty, perUserLimit, flashSaleTTL(act))
		// Redis 异常（aerr != nil）时降级：不阻断，acquiredRedis 保持 false，交由 DB 权威兜底。
		if aerr == nil {
			switch res {
			case cache.FlashSaleSoldOut:
				return nil, ErrFlashSaleSoldOut
			case cache.FlashSaleUserLimit:
				return nil, ErrFlashSaleUserLimit
			case cache.FlashSaleOK:
				acquiredRedis = true
			}
		}
	}
	defer func() {
		// 预扣成功但最终未提交（售罄兜底/积分不足/事务失败等）→ 释放 Redis 名额。
		if acquiredRedis && !committed {
			_ = s.cacheMgr.ReleaseFlashSale(ctx, activityID, userID)
		}
	}()

	// 4. 每人限购 DB 权威校验（弥补无 Redis 或计数漂移场景）。
	cnt, err := s.shopRepo.CountUserFlashOrders(ctx, activityID, userID)
	if err != nil {
		return nil, fmt.Errorf("count user flash orders: %w", err)
	}
	if cnt >= int64(perUserLimit) {
		return nil, ErrFlashSaleUserLimit
	}

	// 5. 校验商品与积分
	item, err := s.shopRepo.FindItemByID(ctx, act.ItemID)
	if err != nil {
		return nil, fmt.Errorf("find item: %w", err)
	}
	if item == nil || !item.IsActive {
		return nil, ErrItemNotFound
	}
	totalPoints := act.PricePoints // 抢购价（覆盖商品原价，可为 0）
	user, err := s.userRepo.FindByID(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("find user: %w", err)
	}
	if user == nil {
		return nil, ErrUserNotFound
	}
	if user.PointsBalance < totalPoints {
		return nil, ErrPointsInsufficient
	}

	// 6. 事务：DB 权威扣配额 + 建订单 + 积分流水 + 积分 Outbox（同提交）
	var outboxRec *model.PointsOutbox
	err = s.businessDB.Transaction(ctx, func(tx *gorm.DB) error {
		shopRepo := repository.NewShopRepositoryFromTx(tx)

		// DB 权威扣减配额：sold_qty<limit_qty 才 +1，已抢光则返回错误回滚（不入库）。
		ok, derr := shopRepo.AcquireFlashQuota(ctx, activityID)
		if derr != nil {
			return fmt.Errorf("acquire flash quota: %w", derr)
		}
		if !ok {
			return ErrFlashSaleSoldOut
		}

		order = &model.RedeemOrder{
			UserID:      userID,
			ItemID:      item.ID,
			ItemName:    item.Name,
			PointsSpent: totalPoints,
			OrderStatus: "completed",
			ActivityID:  activityID,
		}
		if err := shopRepo.CreateOrder(ctx, order); err != nil {
			return fmt.Errorf("create order: %w", err)
		}

		txRecord := &model.PointsTransaction{
			UserID:       userID,
			ChangeAmount: -totalPoints,
			BalanceAfter: user.PointsBalance - totalPoints,
			ChangeType:   model.ChangeTypeRedeemSpend,
			ReferenceID:  fmt.Sprintf("order:%d", order.ID),
		}
		if err := shopRepo.CreateTransaction(ctx, txRecord); err != nil {
			return fmt.Errorf("create points transaction: %w", err)
		}

		rec := &model.PointsOutbox{
			UserID:    userID,
			Delta:     -totalPoints,
			RefType:   "flash_redeem",
			RefID:     fmt.Sprintf("%d", order.ID),
			EventID:   fmt.Sprintf("pts:flash:%d", order.ID),
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
		return nil, err
	}
	committed = true // 事务已提交，不再回滚 Redis 预扣

	// 失效「用户-活动 抢购成功数」缓存：保证每人限购兜底计数即时刷新为最新值，
	// 避免短 TTL 缓存窗口内（Redis 恰巧不可用时）同人二次抢购被错误放行。
	if s.cacheMgr != nil {
		_ = s.cacheMgr.Delete(ctx, cache.FlashUserFlashCountCacheKey(activityID, userID))
	}

	if s.points != nil {
		s.points.ApplyAsync(ctx, outboxRec)
	}
	s.logSvc.LogShopRedeem(ctx, common.BuildMeta(ctx, userID), item.ID, item.Name, totalPoints)
	s.emitShopRedeemed(ctx, userID, order, item, totalPoints)

	return order, nil
}

// flashSaleTTL 计算抢购 Redis 计数器的 TTL：优先取到活动结束的时长（+1h 缓冲），
// 无结束时间时默认 24h，避免计数器长期滞留。
func flashSaleTTL(act *model.ShopFlashActivity) time.Duration {
	if act.EndTime != nil {
		if d := time.Until(*act.EndTime) + time.Hour; d > 0 {
			return d
		}
	}
	return 24 * time.Hour
}

const (
	// flashActivityMetaTTL 活动元信息/列表的三级缓存 TTL。取较短（5s）以在「开抢瞬间
	// 海量读」与「活动状态/库存变更最终一致」之间平衡：5s 内 DB 读取被缓存削减约两个数量级，
	// 且状态/下架变更最多 5s 后对所有客户端可见（见 docs/21）。
	flashActivityMetaTTL = 5 * time.Second
	// flashWarmupLead 调度器预热提前量：开抢前该时长内即开始预热 Redis 库存，
	// 确保首请求无需承担库存初始化开销（见 WarmupDueFlashSales）。
	flashWarmupLead = 2 * time.Minute
	// redeemStockTTL 普通商品 Redis 库存计数器的 TTL。仅作内存上限，
	// 惰性初始化用 DB 当前 stock 自愈，故无需与活动窗口对齐。
	redeemStockTTL = 30 * time.Minute
)

// redeemResultLabel 将普通商品兑换结果映射为指标标签（success/sold_out_peak/sold_out_db/
// points_insufficient/concurrent/item_offline/other）。sold_out_peak 与 sold_out_db 区分
// 削峰层拦截与 DB 行锁售罄，便于观测普通兑换的「削峰拦截占比」（对应 P1-4）。
func redeemResultLabel(err error) string {
	switch err {
	case nil:
		return "success"
	case ErrRedeemSoldOutPeak:
		return "sold_out_peak"
	case ErrStockInsufficient:
		return "sold_out_db"
	case ErrPointsInsufficient:
		return "points_insufficient"
	case ErrRedeemConcurrent:
		return "concurrent"
	case ErrItemNotFound:
		return "item_offline"
	default:
		return "other"
	}
}

// flashResultLabel 将抢购结果映射为指标标签（success/sold_out/user_limit/not_started/ended/timeout/other）。
func flashResultLabel(err error) string {
	// 写事务等待行锁/连接池超过 request_timeout deadline 被取消：单独归为 timeout（业务码 10309），
	// 与 other 区分，便于在 shop_flash_redeem_total{result="timeout"} 上单独观测容量瓶颈。
	if err != nil && errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	switch err {
	case nil:
		return "success"
	case ErrFlashSaleSoldOut:
		return "sold_out"
	case ErrFlashSaleUserLimit:
		return "user_limit"
	case ErrFlashSaleNotStarted:
		return "not_started"
	case ErrFlashSaleEnded:
		return "ended"
	default:
		return "other"
	}
}

// emitShopRedeemed 发布商品兑换事件（仅当配置了 producer）。
func (s *ShopService) emitShopRedeemed(ctx context.Context, userID string, order *model.RedeemOrder, item *model.ShopItem, pointsSpent int64) {
	if s.publisher == nil {
		return
	}
	msg := &event.Message{
		EventType: event.EventShopRedeemed,
		Key:       userID,
		EventID:   fmt.Sprintf("shop:%d", order.ID), // 幂等键：以订单主键派生，天然唯一
		Timestamp: time.Now(),
		Payload: event.ShopRedeemedPayload{
			UserID:      userID,
			OrderID:     order.ID,
			ItemID:      item.ID,
			ItemName:    item.Name,
			PointsSpent: pointsSpent,
		},
	}
	// 事件发布失败不影响兑换结果（Producer 内部已有 DLQ 降级）
	_ = s.publisher.Send(ctx, msg)
}

// Orders 订单列表（游标分页）
func (s *ShopService) Orders(ctx context.Context, userID string, cursor int64, limit int) ([]model.RedeemOrder, int64, bool, error) {
	orders, err := s.shopRepo.FindOrders(ctx, userID, cursor, limit)
	if err != nil {
		return nil, 0, false, fmt.Errorf("find orders: %w", err)
	}
	trimmed, nextCursor, hasMore := common.CursorPage(orders, limit, func(o model.RedeemOrder) int64 { return o.ID })
	return trimmed, nextCursor, hasMore, nil
}

// OrderDetail 订单详情
func (s *ShopService) OrderDetail(ctx context.Context, orderID int64) (*model.RedeemOrder, error) {
	order, err := s.shopRepo.FindOrderByID(ctx, orderID)
	if err != nil {
		return nil, fmt.Errorf("find order: %w", err)
	}
	if order == nil {
		return nil, ErrOrderNotFound
	}
	return order, nil
}

// SeedItems 初始化商品数据
func (s *ShopService) SeedItems() error {
	return s.shopRepo.SeedItems()
}

// BackfillStockBuckets 为存量「有库存但无桶」的商品补齐分桶（P2-5 升级回填，幂等）。
// 启动期由 bootstrap 调用一次，保证存量库也能享受分桶写分散收益。
func (s *ShopService) BackfillStockBuckets(ctx context.Context) error {
	return s.shopRepo.BackfillStockBuckets(ctx)
}

// ReconcileItemRedeemStock 以 DB 权威剩余库存（桶之和）强制重置某普通商品的 Redis 预扣计数器。
// 对应 flash 的 SyncFlashSaleStock：运营改库存、压测重置库存等场景调用，消除 Redis 漂移导致的
// 假售罄（Lua 已改为仅 key 缺失时初始化，运行期不再自动自愈，须显式对账）。L2 未启用时为 no-op。
func (s *ShopService) ReconcileItemRedeemStock(ctx context.Context, itemID int64) error {
	if s.cacheMgr == nil || !s.cacheMgr.L2Enabled() {
		return nil
	}
	total, err := s.shopRepo.TotalStock(ctx, itemID)
	if err != nil {
		return err
	}
	return s.cacheMgr.ReconcileRedeem(ctx, itemID, int64(total), redeemStockTTL)
}

var (
	ErrItemNotFound      = fmt.Errorf("item not found or offline")
	ErrStockInsufficient = fmt.Errorf("stock insufficient")
	// ErrRedeemSoldOutPeak 普通商品兑换被 Redis 削峰层（预扣库存）判定已售罄拦截。
	// 与 ErrStockInsufficient（DB 行锁权威售罄，码 10302）区分，便于容量观测（P1-4 修复）。
	ErrRedeemSoldOutPeak  = fmt.Errorf("redeem sold out (peak-shaving layer)")
	ErrPointsInsufficient = fmt.Errorf("points insufficient")
	ErrUserNotFound       = fmt.Errorf("user not found")
	ErrOrderNotFound      = fmt.Errorf("order not found")
	// ErrRedeemConcurrent 同一用户并发兑换被分布式锁拦截，客户端应稍后重试
	ErrRedeemConcurrent = fmt.Errorf("concurrent redeem in progress, please retry later")

	// 定时抢购相关错误
	ErrFlashSaleNotFound   = fmt.Errorf("flash sale activity not found")
	ErrFlashSaleNotStarted = fmt.Errorf("flash sale not started")
	ErrFlashSaleEnded      = fmt.Errorf("flash sale ended")
	ErrFlashSaleSoldOut    = fmt.Errorf("flash sale sold out")
	ErrFlashSaleUserLimit  = fmt.Errorf("flash sale per-user limit exceeded")
)

// acquireLock 获取分布式锁；cacheMgr 为 nil（未启用缓存）时直接返回成功，由乐观锁兜底。
func (s *ShopService) acquireLock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if s.cacheMgr == nil {
		return true, nil
	}
	return s.cacheMgr.Lock(ctx, key, ttl)
}

// releaseLock 释放分布式锁；cacheMgr 为 nil 时为 no-op。
func (s *ShopService) releaseLock(ctx context.Context, key string) {
	if s.cacheMgr == nil {
		return
	}
	_ = s.cacheMgr.Unlock(ctx, key)
}
