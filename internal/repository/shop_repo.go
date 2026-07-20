package repository

import (
	"context"
	"fmt"
	"hash"
	"hash/fnv"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/cdcdx/hc-framework-go/internal/cache"
	"github.com/cdcdx/hc-framework-go/internal/db"
	"github.com/cdcdx/hc-framework-go/internal/model"
)

// stockBucketCount 单商品库存分桶数（P2-5）。把 shop_items 单行锁竞争分散到 N 行；
// 取值为 2 的幂便于 hash 取模，16 在「分散度」与「桶行元数据量」之间取得平衡。
const stockBucketCount = 16

// bucketIndex 由分片键（userID）稳定映射到桶序号，使同一用户始终优先命中同一桶，
// 不同用户尽量分散到不同桶，最大化降低行锁竞争。
func bucketIndex(shardKey string, n int) int {
	var h hash.Hash32 = fnv.New32a()
	_, _ = h.Write([]byte(shardKey))
	return int(h.Sum32() % uint32(n))
}

// ShopRepository 商城仓库
type ShopRepository struct {
	rw       *db.RWDB
	cacheMgr *cache.Manager // 三级缓存（可选）
}

// NewShopRepository 创建商城仓库（读写分离）
func NewShopRepository(rw *db.RWDB) *ShopRepository {
	return &ShopRepository{rw: rw}
}

// NewShopRepositoryWithCache 创建带缓存的商城仓库
func NewShopRepositoryWithCache(rw *db.RWDB, cacheMgr *cache.Manager) *ShopRepository {
	return &ShopRepository{rw: rw, cacheMgr: cacheMgr}
}

// NewShopRepositoryFromTx 从事务 gorm.DB 创建商城仓库（事务内使用，强制走主库，不使用缓存）
func NewShopRepositoryFromTx(tx *gorm.DB) *ShopRepository {
	return &ShopRepository{rw: db.NewRWDBFromGORM(tx)}
}

// FindItems 获取商品列表（读从库，游标分页 + 分类筛选）
func (r *ShopRepository) FindItems(ctx context.Context, cursor int64, limit int, category string) ([]model.ShopItem, error) {
	if r.rw == nil {
		return nil, fmt.Errorf("shop repo: rw not initialized")
	}
	if limit <= 0 {
		limit = 20
	}
	var items []model.ShopItem
	query := r.rw.Read(ctx).Where("is_active = ?", true)

	if category != "" {
		query = query.Where("category = ?", category)
	}
	if cursor > 0 {
		query = query.Where("id > ?", cursor)
	}

	if err := query.Order("id ASC").Limit(limit + 1).Find(&items).Error; err != nil {
		return nil, err
	}
	// 分桶库存回填：有桶的商品，Stock 以桶之和覆盖（shop_items.stock 不再随扣减实时更新，
	// 仅作兜底）。单次 IN 查询，仅触碰当前列表的商品桶，开销可控。
	r.fillBucketStock(ctx, items)

	// 派生销售状态（仅展示，不落库）：在仓库层填充，保证「缓存命中」与「缓存未命中」两条路径
	// 返回的活动/商品都携带 sales_status，避免列表经三级缓存返回空 sales_status。
	for i := range items {
		items[i].SalesStatus = items[i].SalesStatusValue()
	}
	return items, nil
}

// fillBucketStock 用桶之和覆盖 items 中各商品的 Stock（仅对存在桶的商品生效；无桶商品保持原值）。
// 单次分组查询覆盖整批商品，避免 N 次往返。
func (r *ShopRepository) fillBucketStock(ctx context.Context, items []model.ShopItem) {
	if r.rw == nil {
		return
	}
	if len(items) == 0 {
		return
	}
	ids := make([]int64, len(items))
	for i := range items {
		ids[i] = items[i].ID
	}
	type bucketSum struct {
		ItemID int64 `gorm:"column:item_id"`
		Total  int64 `gorm:"column:total"`
	}
	var rows []bucketSum
	if err := r.rw.Read(ctx).Model(&model.ShopItemStockBucket{}).
		Where("item_id IN ?", ids).
		Select("item_id, COALESCE(SUM(stock),0) AS total").
		Group("item_id").Scan(&rows).Error; err != nil {
		return
	}
	if len(rows) == 0 {
		return
	}
	m := make(map[int64]int, len(rows))
	for _, row := range rows {
		m[row.ItemID] = int(row.Total)
	}
	for i := range items {
		if total, ok := m[items[i].ID]; ok {
			items[i].Stock = total
		}
	}
}

// FindItemByID 根据 ID 获取商品（读从库 → 三级缓存）。
//
// 注意：Redeem 的库存扣减已由 DeductStock 的原子条件更新（WHERE stock>=quantity）兜底，
// 不再依赖读到的 version 做乐观锁，因此此处走缓存是安全的——即便缓存中 stock/version 略有滞后，
// 真正的扣减与防超卖由 DB 行锁保证，不会因缓存旧值而误判库存不足。这样普通兑换路径无需
// 每次强读主库、也无需绕过缓存。
func (r *ShopRepository) FindItemByID(ctx context.Context, id int64) (*model.ShopItem, error) {
	if r.rw == nil {
		return nil, fmt.Errorf("shop repo: rw not initialized")
	}
	cacheKey := fmt.Sprintf("shop:item:%d", id)
	const ttl = 120 * time.Second

	if r.cacheMgr != nil {
		val, err := r.cacheMgr.Get(ctx, cacheKey, ttl, func(ctx context.Context) (interface{}, error) {
			var item model.ShopItem
			dbErr := r.rw.Read(ctx).First(&item, id).Error
			if dbErr != nil {
				if dbErr == gorm.ErrRecordNotFound {
					return nil, nil
				}
				return nil, dbErr
			}
			return &item, nil
		})
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil
		}
		// 缓存命中时 L2(JSON) 反序列化为 map[string]interface{}，需还原为具体类型。
		it, derr := cache.DecodeCached[*model.ShopItem](val)
		if derr != nil {
			return nil, derr
		}
		// 分桶库存回填：展示用 Stock 优先取桶之和（shop_items.stock 不再实时更新）。
		r.fillItemStock(ctx, it)
		return it, nil
	}

	var item model.ShopItem
	err := r.rw.Read(ctx).First(&item, id).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	// 分桶库存回填（无桶商品沿用 shop_items.stock 原值）。
	r.fillItemStock(ctx, &item)
	return &item, nil
}

// fillItemStock 用桶之和覆盖单个商品的 Stock（无桶商品保持原值）。
func (r *ShopRepository) fillItemStock(ctx context.Context, item *model.ShopItem) {
	if r.rw == nil {
		return
	}
	if item == nil {
		return
	}
	total, err := r.TotalStock(ctx, item.ID)
	if err == nil {
		item.Stock = total
	}
}

// TotalStock 返回商品当前可用库存：有桶时取桶之和（分桶权威值），无桶时退回 shop_items.stock。
func (r *ShopRepository) TotalStock(ctx context.Context, id int64) (int, error) {
	if r.rw == nil {
		return 0, fmt.Errorf("shop repo: rw not initialized")
	}
	var cnt int64
	if err := r.rw.Read(ctx).Model(&model.ShopItemStockBucket{}).Where("item_id = ?", id).Count(&cnt).Error; err != nil {
		return 0, err
	}
	if cnt == 0 {
		var it model.ShopItem
		if err := r.rw.Read(ctx).First(&it, id).Error; err != nil {
			return 0, err
		}
		return it.Stock, nil
	}
	var sum int64
	if err := r.rw.Read(ctx).Model(&model.ShopItemStockBucket{}).
		Where("item_id = ?", id).Select("COALESCE(SUM(stock),0)").Scan(&sum).Error; err != nil {
		return 0, err
	}
	return int(sum), nil
}

// DeductStock 原子条件扣减库存（写主库）。
//
// 分桶改造（P2-5）：
//   - 商品存在桶行 → 走分桶扣减：按 hash(shardKey)%N 选主桶，主桶不足时按顺序尝试其余桶，
//     任一桶条件更新（WHERE stock>=quantity）成功即返回。把原本集中在 shop_items 单行的行锁
//     竞争分散到 N 行，显著降低抢兑高峰写热点。桶是权威存储，零超卖由行锁保证。
//   - 商品无桶行（直建/未回填）→ 回退到 shop_items 单行原子扣减（旧语义），保证 item.Stock 准确。
//
// shardKey 一般传 userID，使其稳定命中同一桶、不同用户分散到不同桶。
func (r *ShopRepository) DeductStock(ctx context.Context, id int64, quantity int, shardKey string) error {
	if r.rw == nil {
		return fmt.Errorf("shop repo: rw not initialized")
	}
	var bucketCnt int64
	if err := r.rw.Read(ctx).Model(&model.ShopItemStockBucket{}).Where("item_id = ?", id).Count(&bucketCnt).Error; err != nil {
		return err
	}

	if bucketCnt == 0 {
		// 回退路径：单行原子扣减（保持旧语义，item.Stock 准确）。
		res := r.rw.Write(ctx).Model(&model.ShopItem{}).
			Where("id = ? AND stock >= ?", id, quantity).
			Update("stock", gorm.Expr("stock - ?", quantity))
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return fmt.Errorf("stock insufficient")
		}
		r.invalidateItemCache(ctx, id)
		return nil
	}

	// 分桶路径：主桶优先，不足则顺序尝试其余桶，避免桶间库存倾斜导致“假售罄”。
	primary := bucketIndex(shardKey, int(bucketCnt))
	for attempt := 0; attempt < int(bucketCnt); attempt++ {
		b := (primary + attempt) % int(bucketCnt)
		res := r.rw.Write(ctx).Model(&model.ShopItemStockBucket{}).
			Where("item_id = ? AND bucket = ? AND stock >= ?", id, b, quantity).
			Update("stock", gorm.Expr("stock - ?", quantity))
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected > 0 {
			r.invalidateItemCache(ctx, id)
			return nil
		}
	}
	return fmt.Errorf("stock insufficient")
}

// CreateStockBuckets 把 total 库存均分到 N 个桶行（余数前置到前若干桶）。
// 幂等：已存在桶行时 ON CONFLICT DO NOTHING 跳过，不覆盖已扣减的库存。
// 仅用于种子初始化——已存在的桶不应被重新均分（会丢失已扣减状态）。
func (r *ShopRepository) CreateStockBuckets(ctx context.Context, itemID int64, total int) error {
	if r.rw == nil {
		return fmt.Errorf("shop repo: rw not initialized")
	}
	buckets := makeBuckets(itemID, total, stockBucketCount)
	return r.rw.Master().Clauses(clause.OnConflict{DoNothing: true}).Create(&buckets).Error
}

// makeBuckets 将 total 均分为 n 个桶，余数前置到前 rem 个桶，使总和恰为 total。
func makeBuckets(itemID int64, total, n int) []model.ShopItemStockBucket {
	buckets := make([]model.ShopItemStockBucket, 0, n)
	base := total / n
	rem := total % n
	for b := 0; b < n; b++ {
		s := base
		if b < rem {
			s++
		}
		buckets = append(buckets, model.ShopItemStockBucket{ItemID: itemID, Bucket: b, Stock: s})
	}
	return buckets
}

// BackfillStockBuckets 为「有库存但尚无桶行」的商品补齐分桶（一次性回填，幂等）。
// 已扣减过的库存以当前 shop_items.stock 为基准均分，保证桶之和 == 当前真实可用库存。
// 用于存量库升级：AutoMigrate 建表后由 bootstrap 调用一次。
func (r *ShopRepository) BackfillStockBuckets(ctx context.Context) error {
	if r.rw == nil {
		return fmt.Errorf("shop repo: rw not initialized")
	}
	type idStock struct {
		ID    int64 `gorm:"column:id"`
		Stock int   `gorm:"column:stock"`
	}
	var items []idStock
	if err := r.rw.Read(ctx).Model(&model.ShopItem{}).
		Where("stock > 0 AND id NOT IN (SELECT item_id FROM shop_item_stock_buckets)").
		Select("id, stock").Scan(&items).Error; err != nil {
		return err
	}
	for _, it := range items {
		if err := r.CreateStockBuckets(ctx, it.ID, it.Stock); err != nil {
			return err
		}
	}
	return nil
}

// invalidateItemCache 失效商品缓存
func (r *ShopRepository) invalidateItemCache(ctx context.Context, itemID int64) {
	if r.cacheMgr == nil {
		return
	}
	_ = r.cacheMgr.Delete(ctx, fmt.Sprintf("shop:item:%d", itemID))
}

// CreateOrder 创建兑换订单（写主库）
func (r *ShopRepository) CreateOrder(ctx context.Context, order *model.RedeemOrder) error {
	if r.rw == nil {
		return fmt.Errorf("shop repo: rw not initialized")
	}
	return r.rw.Write(ctx).Create(order).Error
}

// FindOrders 查询订单列表（读从库，游标分页）
func (r *ShopRepository) FindOrders(ctx context.Context, userID string, cursor int64, limit int) ([]model.RedeemOrder, error) {
	if r.rw == nil {
		return nil, fmt.Errorf("shop repo: rw not initialized")
	}
	if limit <= 0 {
		limit = 20
	}
	var orders []model.RedeemOrder
	query := r.rw.Read(ctx).
		Where("user_id = ?", userID).
		Order("id DESC").
		Limit(limit + 1)

	if cursor > 0 {
		query = query.Where("id < ?", cursor)
	}

	if err := query.Find(&orders).Error; err != nil {
		return nil, err
	}
	return orders, nil
}

// FindOrderByID 根据订单 ID 查询（读从库）
func (r *ShopRepository) FindOrderByID(ctx context.Context, orderID int64) (*model.RedeemOrder, error) {
	if r.rw == nil {
		return nil, fmt.Errorf("shop repo: rw not initialized")
	}
	var order model.RedeemOrder
	err := r.rw.Read(ctx).First(&order, orderID).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &order, nil
}

// CreateTransaction 创建积分流水（写主库）
func (r *ShopRepository) CreateTransaction(ctx context.Context, tx *model.PointsTransaction) error {
	if r.rw == nil {
		return fmt.Errorf("shop repo: rw not initialized")
	}
	return r.rw.Write(ctx).Create(tx).Error
}

// ────────── 定时抢购活动 ──────────

// FindActivityByID 根据 ID 查询抢购活动（读从库）。
func (r *ShopRepository) FindActivityByID(ctx context.Context, id int64) (*model.ShopFlashActivity, error) {
	if r.rw == nil {
		return nil, fmt.Errorf("shop repo: rw not initialized")
	}
	var act model.ShopFlashActivity
	err := r.rw.Read(ctx).First(&act, id).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &act, nil
}

// FindActivities 查询抢购活动列表。
// includeEnded=false 时只返回进行中（状态 active 且在时间窗内）；true 时额外包含已结束（运营后台全量视角）。
// 读从库，按开始时间倒序。
func (r *ShopRepository) FindActivities(ctx context.Context, includeEnded bool, limit int) ([]model.ShopFlashActivity, error) {
	if r.rw == nil {
		return nil, fmt.Errorf("shop repo: rw not initialized")
	}
	if limit <= 0 {
		limit = 20
	}
	now := time.Now()
	q := r.rw.Read(ctx).Model(&model.ShopFlashActivity{})
	if !includeEnded {
		// 用户侧：展示未结束的活动，含「预售中(pending 且未到开抢)」与「进行中(active 且已开抢)」；
		// 已结束(ended)或已过结束时间的不展示。start_time 已到但未翻 pending→active 的极小窗口内
		// 暂不展示，等待调度器校正状态后再出现，避免列表显示 presale 却实际已可抢的不一致。
		q = q.Where("end_time IS NULL OR end_time >= ?", now).
			Where("(status = ? AND start_time > ?) OR (status = ? AND start_time <= ?)",
				model.FlashSaleStatusPending, now, model.FlashSaleStatusActive, now)
	}
	var acts []model.ShopFlashActivity
	if err := q.Order("start_time DESC").Limit(limit).Find(&acts).Error; err != nil {
		return nil, err
	}
	// 派生销售状态（仅展示，不落库）：在仓库层填充，保证「缓存命中」与「缓存未命中」两条路径
	// 返回的活动都携带 sales_status，避免列表经三级缓存返回空 sales_status（此前表现为
	// 客户端收到 sales_status=""）。
	for i := range acts {
		acts[i].SalesStatus = acts[i].SalesStatusValue()
	}
	return acts, nil
}

// ListWarmupCandidates 返回需要预热/对账的抢购活动（供调度器自动预热）：
// 状态为 active 或 pending，且 (end_time 为零 或 end_time >= now)，且 start_time 落在
// [now-lead, now+lead] 窗口内（覆盖「即将开抢」与「已开始但预热/对账遗漏」两类）。
func (r *ShopRepository) ListWarmupCandidates(ctx context.Context, now time.Time, lead time.Duration) ([]model.ShopFlashActivity, error) {
	if r.rw == nil {
		return nil, fmt.Errorf("shop repo: rw not initialized")
	}
	var acts []model.ShopFlashActivity
	err := r.rw.Read(ctx).Model(&model.ShopFlashActivity{}).
		Where("status IN ?", []string{model.FlashSaleStatusActive, model.FlashSaleStatusPending}).
		Where("start_time BETWEEN ? AND ?", now.Add(-lead), now.Add(lead)).
		Where("end_time IS NULL OR end_time >= ?", now).
		Order("start_time ASC").
		Find(&acts).Error
	return acts, err
}

// TransitionStaleStatuses 周期校正抢购活动生命周期状态（幂等）：
//   - 已到开抢时间（start_time <= now）的 pending 活动 → active（让预售活动到点后出现在用户列表）；
//   - 已到结束时间（end_time <= now）的 active 活动 → ended。
//
// 仅更新 Status 字段，不动 sold_qty 等运行期权威计数；已是目标状态的活动不受影响。
func (r *ShopRepository) TransitionStaleStatuses(ctx context.Context, now time.Time) (int64, error) {
	if r.rw == nil {
		return 0, fmt.Errorf("shop repo: rw not initialized")
	}
	res := r.rw.Write(ctx).Model(&model.ShopFlashActivity{}).
		Where("status = ? AND start_time <= ?", model.FlashSaleStatusPending, now).
		Update("status", model.FlashSaleStatusActive)
	if res.Error != nil {
		return 0, res.Error
	}
	n := res.RowsAffected
	res2 := r.rw.Write(ctx).Model(&model.ShopFlashActivity{}).
		Where("status = ? AND end_time IS NOT NULL AND end_time <= ?", model.FlashSaleStatusActive, now).
		Update("status", model.FlashSaleStatusEnded)
	if res2.Error != nil {
		return n, res2.Error
	}
	return n + res2.RowsAffected, nil
}

// CreateActivity 创建抢购活动（写主库，供运营配置）。
func (r *ShopRepository) CreateActivity(ctx context.Context, act *model.ShopFlashActivity) error {
	if r.rw == nil {
		return fmt.Errorf("shop repo: rw not initialized")
	}
	return r.rw.Write(ctx).Create(act).Error
}

// UpdateActivity 更新抢购活动（写主库，供运营调整限量/每人限购/时间窗/状态等）。
// Updates 显式列出可改字段，避免误改 sold_qty 等运行期权威计数。
func (r *ShopRepository) UpdateActivity(ctx context.Context, act *model.ShopFlashActivity) error {
	if r.rw == nil {
		return fmt.Errorf("shop repo: rw not initialized")
	}
	return r.rw.Write(ctx).Model(&model.ShopFlashActivity{}).
		Where("id = ?", act.ID).
		Updates(map[string]interface{}{
			"name":           act.Name,
			"start_time":     act.StartTime,
			"end_time":       act.EndTime,
			"limit_qty":      act.LimitQty,
			"per_user_limit": act.PerUserLimit,
			"price_points":   act.PricePoints,
			"status":         act.Status,
		}).Error
}

// AcquireFlashQuota DB 权威扣减抢购配额（防超卖的最终防线）。
// 条件更新：仅当 sold_qty < limit_qty 时 +1，行锁天然串行，RowsAffected==0 即已抢光。
// 返回 (true, nil) 表示扣减成功；(false, nil) 表示已抢光。
func (r *ShopRepository) AcquireFlashQuota(ctx context.Context, activityID int64) (bool, error) {
	if r.rw == nil {
		return false, fmt.Errorf("shop repo: rw not initialized")
	}
	result := r.rw.Write(ctx).Model(&model.ShopFlashActivity{}).
		Where("id = ? AND sold_qty < limit_qty", activityID).
		Update("sold_qty", gorm.Expr("sold_qty + 1"))
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

// ReleaseFlashQuota 回滚一次 DB 配额扣减（入库事务失败时释放，sold_qty>0 才回退）。
func (r *ShopRepository) ReleaseFlashQuota(ctx context.Context, activityID int64) error {
	if r.rw == nil {
		return fmt.Errorf("shop repo: rw not initialized")
	}
	return r.rw.Write(ctx).Model(&model.ShopFlashActivity{}).
		Where("id = ? AND sold_qty > 0", activityID).
		Update("sold_qty", gorm.Expr("sold_qty - 1")).Error
}

// CountUserFlashOrders 统计用户在某抢购活动下已成功的订单数（每人限购的 DB 权威校验）。
// 叠加一层短 TTL（FlashUserFlashCountTTL=3s）三级缓存：开抢瞬间 1000 VU 并发抢购时，每个用户
// 经 Redis 预扣放行后都会查一次本计数；若查询因 DB 连接池饱和报错（10003），请求失败释放 Redis
// 名额后会被 VU 重试，重试再次命中本查询 → 形成「报错-重试」正反馈放大 DB 压力。短缓存使重试在
// TTL 内命中缓存、不再回源，切断放大回路，显著减少尖峰期 10003。
//
// 注意：Redis 预扣（TryAcquireFlashSale）才是每人限购的主防线；本计数仅为 Redis 不可用/计数漂移时
// 的兜底。缓存 TTL 较短，且抢购成功后会失效该 key（见 ShopService.FlashRedeem 的 committed 路径），
// 保证兜底计数在成功下单后即时刷新，不会放行同人短时间内二次抢购（即便 Redis 恰巧宕机）。
func (r *ShopRepository) CountUserFlashOrders(ctx context.Context, activityID int64, userID string) (int64, error) {
	if r.rw == nil {
		return 0, fmt.Errorf("shop repo: rw not initialized")
	}
	if r.cacheMgr != nil && r.cacheMgr.L2Enabled() {
		key := cache.FlashUserFlashCountCacheKey(activityID, userID)
		load := func(c context.Context) (interface{}, error) {
			var cnt int64
			if err := r.rw.Read(c).Model(&model.RedeemOrder{}).
				Where("activity_id = ? AND user_id = ?", activityID, userID).
				Count(&cnt).Error; err != nil {
				return nil, err
			}
			return cnt, nil
		}
		if val, err := r.cacheMgr.Get(ctx, key, cache.FlashUserFlashCountTTL, load); err == nil {
			if cnt, e := cache.DecodeCached[int64](val); e == nil {
				return cnt, nil
			}
		}
		// 缓存不可用/解码失败：降级为直接查 DB（与无缓存行为一致）。
	}
	var cnt int64
	err := r.rw.Read(ctx).Model(&model.RedeemOrder{}).
		Where("activity_id = ? AND user_id = ?", activityID, userID).
		Count(&cnt).Error
	return cnt, err
}

// Transaction 在主库上执行事务
func (r *ShopRepository) Transaction(ctx context.Context, fn func(tx *gorm.DB) error) error {
	if r.rw == nil {
		return fmt.Errorf("shop repo: rw not initialized")
	}
	return r.rw.Transaction(ctx, fn)
}

// SeedItems 初始化默认商品数据（写主库）
func (r *ShopRepository) SeedItems() error {
	if r.rw == nil {
		return fmt.Errorf("shop repo: rw not initialized")
	}
	var count int64
	r.rw.Master().Model(&model.ShopItem{}).Count(&count)
	if count > 0 {
		return nil
	}

	items := []model.ShopItem{
		{Name: "100 积分券", Description: "兑换 100 积分", PricePoints: 0, Stock: 999999, Version: 0, Category: "coupon", IsActive: true},
		{Name: "VIP 体验卡 (1天)", Description: "享受 VIP 特权 1 天", PricePoints: 500, Stock: 1000, Version: 0, Category: "vip", IsActive: true},
		{Name: "挂机加速卡 (1小时)", Description: "挂机积分翻倍 1 小时", PricePoints: 300, Stock: 500, Version: 0, Category: "boost", IsActive: true},
		{Name: "限定头像框", Description: "炫酷的限定头像框", PricePoints: 1000, Stock: 100, Version: 0, Category: "cosmetic", IsActive: true},
		{Name: "抽奖券 x5", Description: "5 张抽奖券", PricePoints: 200, Stock: 5000, Version: 0, Category: "coupon", IsActive: true},
	}

	itemIDs := make([]int64, 0)
	for i := range items {
		// 对同一商品去重
		var existing model.ShopItem
		if err := r.rw.Master().Where("name = ?", items[i].Name).First(&existing).Error; err == nil {
			continue
		}
		if err := r.rw.Master().Create(&items[i]).Error; err != nil {
			return err
		}
		itemIDs = append(itemIDs, items[i].ID)
		// 同步建库存分桶（P2-5）：把种子库存均分到 N 个桶行，使抢兑写竞争分散。
		if err := r.CreateStockBuckets(context.Background(), items[i].ID, items[i].Stock); err != nil {
			return err
		}
	}
	// 写入后批量失效缓存
	for _, id := range itemIDs {
		r.invalidateItemCache(context.Background(), id)
	}
	return nil
}

// clause 引用（保留给未来扩展使用）
var _ = clause.Locking{Strength: "UPDATE"}

// AutoMigrate 自动迁移（DDL 操作主库）
func (r *ShopRepository) AutoMigrate() error {
	if r.rw == nil {
		return fmt.Errorf("shop repo: rw not initialized")
	}
	return r.rw.Master().AutoMigrate(&model.ShopItem{}, &model.RedeemOrder{}, &model.PointsTransaction{}, &model.ShopFlashActivity{}, &model.ShopItemStockBucket{})
}
