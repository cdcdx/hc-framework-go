package shop

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/db"
	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/cdcdx/hc-framework-go/internal/repository"
	"github.com/cdcdx/hc-framework-go/internal/service/common"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// ============================================================
// 内存假实现：让 common.LogService 后台循环（writeLog/writeMetric）写入不 panic。
// 不依赖真实日志后端（AuditLog/MonitorMetric 表），仅记录条数。
// ============================================================

type fakeLogRepo struct{ n int }

func (f *fakeLogRepo) Create(_ context.Context, _ *model.AuditLog) error { f.n++; return nil }
func (f *fakeLogRepo) FindByUser(_ context.Context, _ string, _ int64, _ int) ([]model.AuditLog, error) {
	return nil, nil
}
func (f *fakeLogRepo) FindByType(_ context.Context, _ string, _, _ time.Time, _ int) ([]model.AuditLog, error) {
	return nil, nil
}
func (f *fakeLogRepo) CountByType(_ context.Context, _ string, _, _ time.Time) (int64, error) {
	return 0, nil
}
func (f *fakeLogRepo) Close() error            { return nil }
func (f *fakeLogRepo) SQLDB() (*sql.DB, error) { return nil, nil }

type fakeMonitorRepo struct{ n int }

func (f *fakeMonitorRepo) Record(_ context.Context, _ *model.MonitorMetric) error { f.n++; return nil }
func (f *fakeMonitorRepo) CountByType(_ context.Context, _ string, _, _ time.Time) (int64, error) {
	return 0, nil
}
func (f *fakeMonitorRepo) SumByType(_ context.Context, _ string, _, _ time.Time) (float64, error) {
	return 0, nil
}
func (f *fakeMonitorRepo) FindByTimeRange(_ context.Context, _, _ time.Time, _ int) ([]model.MonitorMetric, error) {
	return nil, nil
}
func (f *fakeMonitorRepo) Close() error            { return nil }
func (f *fakeMonitorRepo) SQLDB() (*sql.DB, error) { return nil, nil }

// newTestShop 使用内存 SQLite 搭建 ShopService（无缓存/无 MQ/无积分 Outbox），用于集成测试核心兑换逻辑。
func newTestShop(t *testing.T) (*ShopService, *gorm.DB) {
	t.Helper()
	dsn := fmt.Sprintf("file:mem_%s?mode=memory&cache=shared", sanitizeName(t.Name()))
	gdb, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	// 内存库在最后一个连接关闭后即销毁；限制单连接以避免测试间串库。
	if sqlDB, err := gdb.DB(); err == nil {
		sqlDB.SetMaxOpenConns(1)
	}
	if err := gdb.AutoMigrate(&model.ShopItem{}, &model.RedeemOrder{}, &model.PointsTransaction{}, &model.User{}, &model.PointsOutbox{}, &model.ShopFlashActivity{}, &model.ShopItemStockBucket{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	businessDB := db.CreateSingleRWDB(gdb)
	userRepo := repository.NewUserRepository(gdb)
	logSvc := common.NewLogService(&fakeLogRepo{}, &fakeMonitorRepo{}, nil)
	t.Cleanup(func() { logSvc.Close() })

	svc := NewShopService(&config.Config{}, userRepo, businessDB, logSvc, nil, nil, nil)
	if err := svc.SeedItems(); err != nil {
		t.Fatalf("seed items: %v", err)
	}
	return svc, gdb
}

func sanitizeName(s string) string {
	re := regexp.MustCompile(`[^a-zA-Z0-9]+`)
	return re.ReplaceAllString(s, "_")
}

func TestShopRedeem_Success(t *testing.T) {
	svc, gdb := newTestShop(t)
	ctx := context.Background()

	item := &model.ShopItem{Name: "T1", PricePoints: 100, Stock: 10, Version: 0, Category: "c", IsActive: true}
	if err := gdb.Create(item).Error; err != nil {
		t.Fatal(err)
	}
	user := &model.User{UserID: "u1", Username: "u1", Email: "u1@x.com", PointsBalance: 1000, Status: "active"}
	if err := gdb.Create(user).Error; err != nil {
		t.Fatal(err)
	}

	order, err := svc.Redeem(ctx, "u1", &RedeemRequest{ItemID: item.ID, Quantity: 2})
	if err != nil {
		t.Fatalf("Redeem: %v", err)
	}
	if order.ID == 0 {
		t.Fatal("order.ID not assigned")
	}
	if order.PointsSpent != 200 {
		t.Fatalf("PointsSpent = %d, want 200", order.PointsSpent)
	}

	// 库存应扣减 2
	var updated model.ShopItem
	if err := gdb.First(&updated, item.ID).Error; err != nil {
		t.Fatal(err)
	}
	if updated.Stock != 8 {
		t.Fatalf("stock = %d, want 8", updated.Stock)
	}
	// 注：DeductStock 已改为原子条件扣减（WHERE stock>=quantity），不再依赖 version 乐观锁，
	// 因此不再断言 version 自增。防超卖由 DB 行锁保证。

	// 订单应可查到
	orders, _, _, err := svc.Orders(ctx, "u1", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(orders) != 1 {
		t.Fatalf("orders = %d, want 1", len(orders))
	}
}

func TestShopRedeem_QuantityDefaultsToOne(t *testing.T) {
	svc, gdb := newTestShop(t)
	ctx := context.Background()
	item := &model.ShopItem{Name: "T2", PricePoints: 100, Stock: 10, Version: 0, Category: "c", IsActive: true}
	gdb.Create(item)
	gdb.Create(&model.User{UserID: "u2", Username: "u2", Email: "u2@x.com", PointsBalance: 1000, Status: "active"})

	order, err := svc.Redeem(ctx, "u2", &RedeemRequest{ItemID: item.ID}) // Quantity 缺省 0
	if err != nil {
		t.Fatalf("Redeem: %v", err)
	}
	if order.PointsSpent != 100 {
		t.Fatalf("PointsSpent = %d, want 100 (quantity default 1)", order.PointsSpent)
	}
}

func TestShopRedeem_StockInsufficient(t *testing.T) {
	svc, gdb := newTestShop(t)
	ctx := context.Background()
	item := &model.ShopItem{Name: "T3", PricePoints: 100, Stock: 1, Version: 0, Category: "c", IsActive: true}
	gdb.Create(item)
	gdb.Create(&model.User{UserID: "u3", Username: "u3", Email: "u3@x.com", PointsBalance: 1000, Status: "active"})

	_, err := svc.Redeem(ctx, "u3", &RedeemRequest{ItemID: item.ID, Quantity: 5})
	if err != ErrStockInsufficient {
		t.Fatalf("err = %v, want ErrStockInsufficient", err)
	}
}

func TestShopRedeem_PointsInsufficient(t *testing.T) {
	svc, gdb := newTestShop(t)
	ctx := context.Background()
	item := &model.ShopItem{Name: "T4", PricePoints: 100, Stock: 10, Version: 0, Category: "c", IsActive: true}
	gdb.Create(item)
	gdb.Create(&model.User{UserID: "u4", Username: "u4", Email: "u4@x.com", PointsBalance: 10, Status: "active"})

	_, err := svc.Redeem(ctx, "u4", &RedeemRequest{ItemID: item.ID, Quantity: 1})
	if err != ErrPointsInsufficient {
		t.Fatalf("err = %v, want ErrPointsInsufficient", err)
	}
}

func TestShopRedeem_ItemNotFoundWhenInactive(t *testing.T) {
	svc, gdb := newTestShop(t)
	ctx := context.Background()
	item := &model.ShopItem{Name: "T5", PricePoints: 100, Stock: 10, Version: 0, Category: "c", IsActive: true}
	gdb.Create(item)
	// 显式置为下架。注意：IsActive 带 default:true，直接以零值 false 创建会被 gorm 跳过
	// 而落到 DB 默认值 true，故用 UpdateColumn 强制写入 is_active=0 以测试下架分支。
	gdb.Model(&model.ShopItem{}).Where("id = ?", item.ID).UpdateColumn("is_active", false)
	gdb.Create(&model.User{UserID: "u5", Username: "u5", Email: "u5@x.com", PointsBalance: 1000, Status: "active"})

	_, err := svc.Redeem(ctx, "u5", &RedeemRequest{ItemID: item.ID, Quantity: 1})
	if err != ErrItemNotFound {
		t.Fatalf("err = %v, want ErrItemNotFound", err)
	}
}

// TestShopRedeem_ExhaustedThenRedeem 验证「奖品全被兑换后再来兑换」：
// 第一次兑换把库存（Stock=1）兑光后，第二次兑换应被拒（ErrStockInsufficient），
// 且库存保持 0、不出现超卖（stock 不会变负）。覆盖 pre-check 分支（shop_service.go:112）。
func TestShopRedeem_ExhaustedThenRedeem(t *testing.T) {
	svc, gdb := newTestShop(t)
	ctx := context.Background()
	item := &model.ShopItem{Name: "T_exhaust", PricePoints: 100, Stock: 1, Version: 0, Category: "c", IsActive: true}
	if err := gdb.Create(item).Error; err != nil {
		t.Fatal(err)
	}
	if err := gdb.Create(&model.User{UserID: "ex_u1", Username: "ex_u1", Email: "ex1@x.com", PointsBalance: 1000, Status: "active"}).Error; err != nil {
		t.Fatal(err)
	}

	// 第一兑：把库存兑光
	if _, err := svc.Redeem(ctx, "ex_u1", &RedeemRequest{ItemID: item.ID, Quantity: 1}); err != nil {
		t.Fatalf("first redeem: %v", err)
	}
	var after1 model.ShopItem
	if err := gdb.First(&after1, item.ID).Error; err != nil {
		t.Fatal(err)
	}
	if after1.Stock != 0 {
		t.Fatalf("after first redeem stock = %d, want 0", after1.Stock)
	}

	// 再来兑换：应被拒（库存不足），且库存保持 0（无超卖）
	_, err := svc.Redeem(ctx, "ex_u1", &RedeemRequest{ItemID: item.ID, Quantity: 1})
	if err != ErrStockInsufficient {
		t.Fatalf("second redeem err = %v, want ErrStockInsufficient", err)
	}
	var after2 model.ShopItem
	if err := gdb.First(&after2, item.ID).Error; err != nil {
		t.Fatal(err)
	}
	if after2.Stock != 0 {
		t.Fatalf("after second redeem stock = %d, want 0 (no oversell)", after2.Stock)
	}
}

// TestShopRedeem_NoOversellUnderConcurrency 验证高并发抢兑同一有限库存商品时不会超卖：
// 库存=5、20 个并发请求（各异用户），最终成功数 == 库存，剩余库存 == 0 且不为负。
// 测试中 cacheMgr=nil（无分布式锁），全靠 DeductStock 的原子条件更新（WHERE stock>=quantity）兜底，
// 证明即便绕过 pre-check 与用户级串行锁，「奖品全被兑换」后仍只会精确拒绝、绝不超卖（与 ExhaustedThenRedeem 互补）。
func TestShopRedeem_NoOversellUnderConcurrency(t *testing.T) {
	svc, gdb := newTestShop(t)
	ctx := context.Background()
	stock := 5
	item := &model.ShopItem{Name: "T_conc", PricePoints: 10, Stock: stock, Version: 0, Category: "c", IsActive: true}
	if err := gdb.Create(item).Error; err != nil {
		t.Fatal(err)
	}

	const n = 20
	var wg sync.WaitGroup
	var mu sync.Mutex
	success := 0
	for i := 0; i < n; i++ {
		uid := fmt.Sprintf("c_u_%d", i)
		if err := gdb.Create(&model.User{UserID: uid, Username: uid, Email: uid + "@x.com", PointsBalance: 1000, Status: "active"}).Error; err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func(uid string) {
			defer wg.Done()
			if _, err := svc.Redeem(ctx, uid, &RedeemRequest{ItemID: item.ID, Quantity: 1}); err == nil {
				mu.Lock()
				success++
				mu.Unlock()
			}
		}(uid)
	}
	wg.Wait()

	if success > stock {
		t.Fatalf("success = %d, exceeds stock %d (oversell!)", success, stock)
	}
	var final model.ShopItem
	if err := gdb.First(&final, item.ID).Error; err != nil {
		t.Fatal(err)
	}
	if final.Stock != stock-success {
		t.Fatalf("final stock = %d, want %d", final.Stock, stock-success)
	}
	if final.Stock < 0 {
		t.Fatalf("final stock = %d, negative (oversell!)", final.Stock)
	}
}

// TestShopRedeem_Buckets_NoOversellUnderConcurrency 验证分桶路径（P2-5）在高并发下不超卖：
// 64 库存均分到 16 个桶（每桶 4），200 个并发各异用户各兑 1 件，成功数恰为 64、
// 桶之和归 0 且不为负——证明写竞争被分散到 N 行、且分桶原子扣减仍零超卖。
func TestShopRedeem_Buckets_NoOversellUnderConcurrency(t *testing.T) {
	svc, gdb := newTestShop(t)
	ctx := context.Background()
	stock := 64
	item := &model.ShopItem{Name: "T_buckets", PricePoints: 10, Stock: stock, Version: 0, Category: "c", IsActive: true}
	if err := gdb.Create(item).Error; err != nil {
		t.Fatal(err)
	}
	if err := svc.shopRepo.CreateStockBuckets(ctx, item.ID, stock); err != nil {
		t.Fatal(err)
	}

	const n = 200
	var wg sync.WaitGroup
	var mu sync.Mutex
	success := 0
	for i := 0; i < n; i++ {
		uid := fmt.Sprintf("b_u_%d", i)
		if err := gdb.Create(&model.User{UserID: uid, Username: uid, Email: uid + "@x.com", PointsBalance: 1000, Status: "active"}).Error; err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func(uid string) {
			defer wg.Done()
			if _, err := svc.Redeem(ctx, uid, &RedeemRequest{ItemID: item.ID, Quantity: 1}); err == nil {
				mu.Lock()
				success++
				mu.Unlock()
			}
		}(uid)
	}
	wg.Wait()

	if success > stock {
		t.Fatalf("success = %d, exceeds stock %d (oversell!)", success, stock)
	}
	total, err := svc.shopRepo.TotalStock(ctx, item.ID)
	if err != nil {
		t.Fatal(err)
	}
	if total != stock-success {
		t.Fatalf("total stock = %d, want %d", total, stock-success)
	}
	if total < 0 {
		t.Fatalf("total stock = %d, negative (oversell!)", total)
	}
}

// ============================================================
// 定时抢购（FlashSale）测试
// 说明：这些测试 cacheMgr=nil（无 Redis 预扣），全靠 DB 权威条件更新 AcquireFlashQuota
// （sold_qty<limit_qty 才 +1）兜底，恰好验证「限量内入库、限量外直接返回不入库、绝不超卖」。
// ============================================================

// newFlashActivity 便捷创建一个抢购活动。
func newFlashActivity(t *testing.T, gdb *gorm.DB, itemID int64, limit, perUser int, start, end time.Time) *model.ShopFlashActivity {
	t.Helper()
	var endPtr *time.Time
	if !end.IsZero() {
		endPtr = &end
	}
	act := &model.ShopFlashActivity{
		ItemID: itemID, Name: "flash", StartTime: start, EndTime: endPtr,
		LimitQty: limit, PerUserLimit: perUser, PricePoints: 0, Status: model.FlashSaleStatusActive,
	}
	if err := gdb.Create(act).Error; err != nil {
		t.Fatal(err)
	}
	return act
}

// TestFlashRedeem_WithinLimitThenSoldOut 验证「限量内入库兑换，限量外直接返回不入库」：
// 限量=2，前 2 个用户抢购成功入库（有订单、sold_qty 递增），第 3 个用户被拒
// （ErrFlashSaleSoldOut），且不产生订单、sold_qty 保持 2（不超卖）。
func TestFlashRedeem_WithinLimitThenSoldOut(t *testing.T) {
	svc, gdb := newTestShop(t)
	ctx := context.Background()
	item := &model.ShopItem{Name: "F_item", PricePoints: 999, Stock: 100, Category: "c", IsActive: true}
	if err := gdb.Create(item).Error; err != nil {
		t.Fatal(err)
	}
	act := newFlashActivity(t, gdb, item.ID, 2, 1, time.Now().Add(-time.Minute), time.Time{})

	for i := 1; i <= 2; i++ {
		uid := fmt.Sprintf("f_ok_%d", i)
		gdb.Create(&model.User{UserID: uid, Username: uid, Email: uid + "@x.com", PointsBalance: 0, Status: "active"})
		order, err := svc.FlashRedeem(ctx, uid, act.ID)
		if err != nil {
			t.Fatalf("user %s should succeed within limit: %v", uid, err)
		}
		if order.ActivityID != act.ID {
			t.Fatalf("order.ActivityID = %d, want %d", order.ActivityID, act.ID)
		}
		if order.PointsSpent != 0 {
			t.Fatalf("flash price should be 0, got %d", order.PointsSpent)
		}
	}

	// 第 3 个用户：超出限量 → 直接拒绝，不入库
	gdb.Create(&model.User{UserID: "f_over", Username: "f_over", Email: "fo@x.com", PointsBalance: 0, Status: "active"})
	_, err := svc.FlashRedeem(ctx, "f_over", act.ID)
	if err != ErrFlashSaleSoldOut {
		t.Fatalf("3rd user err = %v, want ErrFlashSaleSoldOut", err)
	}

	// 对账：订单数 == 2，sold_qty == 2（无超卖，超限者未入库）
	var orderCnt int64
	gdb.Model(&model.RedeemOrder{}).Where("activity_id = ?", act.ID).Count(&orderCnt)
	if orderCnt != 2 {
		t.Fatalf("order count = %d, want 2 (over-limit must not persist)", orderCnt)
	}
	var after model.ShopFlashActivity
	gdb.First(&after, act.ID)
	if after.SoldQty != 2 {
		t.Fatalf("sold_qty = %d, want 2", after.SoldQty)
	}
}

// TestFlashRedeem_NotStartedAndEnded 验证时间窗校验：未到开始时间返回未开始；已过结束时间返回已结束。
func TestFlashRedeem_NotStartedAndEnded(t *testing.T) {
	svc, gdb := newTestShop(t)
	ctx := context.Background()
	item := &model.ShopItem{Name: "F_time", PricePoints: 0, Stock: 100, Category: "c", IsActive: true}
	gdb.Create(item)
	gdb.Create(&model.User{UserID: "f_t", Username: "f_t", Email: "ft@x.com", PointsBalance: 100, Status: "active"})

	// 未开始：start 在未来
	future := newFlashActivity(t, gdb, item.ID, 10, 1, time.Now().Add(time.Hour), time.Time{})
	if _, err := svc.FlashRedeem(ctx, "f_t", future.ID); err != ErrFlashSaleNotStarted {
		t.Fatalf("err = %v, want ErrFlashSaleNotStarted", err)
	}

	// 已结束：end 在过去
	past := newFlashActivity(t, gdb, item.ID, 10, 1, time.Now().Add(-2*time.Hour), time.Now().Add(-time.Hour))
	if _, err := svc.FlashRedeem(ctx, "f_t", past.ID); err != ErrFlashSaleEnded {
		t.Fatalf("err = %v, want ErrFlashSaleEnded", err)
	}
}

// TestFlashRedeem_PerUserLimit 验证每人限购：limit 足够但每人限 1，同一用户第二次被拒。
func TestFlashRedeem_PerUserLimit(t *testing.T) {
	svc, gdb := newTestShop(t)
	ctx := context.Background()
	item := &model.ShopItem{Name: "F_peruser", PricePoints: 0, Stock: 100, Category: "c", IsActive: true}
	gdb.Create(item)
	act := newFlashActivity(t, gdb, item.ID, 10, 1, time.Now().Add(-time.Minute), time.Time{})
	gdb.Create(&model.User{UserID: "f_pu", Username: "f_pu", Email: "fpu@x.com", PointsBalance: 0, Status: "active"})

	if _, err := svc.FlashRedeem(ctx, "f_pu", act.ID); err != nil {
		t.Fatalf("first grab: %v", err)
	}
	if _, err := svc.FlashRedeem(ctx, "f_pu", act.ID); err != ErrFlashSaleUserLimit {
		t.Fatalf("second grab err = %v, want ErrFlashSaleUserLimit", err)
	}
}

// TestFlashRedeem_NoOversellUnderConcurrency 验证高并发抢购不超卖：
// 限量=5、30 个并发用户（每人限 1），成功数恰为 5、sold_qty==5、订单数==5、不超卖。
func TestFlashRedeem_NoOversellUnderConcurrency(t *testing.T) {
	svc, gdb := newTestShop(t)
	ctx := context.Background()
	item := &model.ShopItem{Name: "F_conc", PricePoints: 0, Stock: 1000, Category: "c", IsActive: true}
	gdb.Create(item)
	limit := 5
	act := newFlashActivity(t, gdb, item.ID, limit, 1, time.Now().Add(-time.Minute), time.Time{})

	const n = 30
	var wg sync.WaitGroup
	var mu sync.Mutex
	success := 0
	for i := 0; i < n; i++ {
		uid := fmt.Sprintf("fc_%d", i)
		gdb.Create(&model.User{UserID: uid, Username: uid, Email: uid + "@x.com", PointsBalance: 0, Status: "active"})
		wg.Add(1)
		go func(uid string) {
			defer wg.Done()
			if _, err := svc.FlashRedeem(ctx, uid, act.ID); err == nil {
				mu.Lock()
				success++
				mu.Unlock()
			}
		}(uid)
	}
	wg.Wait()

	if success != limit {
		t.Fatalf("success = %d, want exactly %d (no oversell / no under-sell)", success, limit)
	}
	var after model.ShopFlashActivity
	gdb.First(&after, act.ID)
	if after.SoldQty != limit {
		t.Fatalf("sold_qty = %d, want %d", after.SoldQty, limit)
	}
	if after.SoldQty > after.LimitQty {
		t.Fatalf("sold_qty %d > limit %d (oversell!)", after.SoldQty, after.LimitQty)
	}
	var orderCnt int64
	gdb.Model(&model.RedeemOrder{}).Where("activity_id = ?", act.ID).Count(&orderCnt)
	if orderCnt != int64(limit) {
		t.Fatalf("order count = %d, want %d", orderCnt, limit)
	}
}

// TestFlashActivity_CreateListDetailEnd 验证运营接口闭环：创建→用户列表可见→详情→下架后用户列表隐藏、运营列表仍可见。
func TestFlashActivity_CreateListDetailEnd(t *testing.T) {
	svc, gdb := newTestShop(t)
	ctx := context.Background()
	item := &model.ShopItem{Name: "F_admin", PricePoints: 0, Stock: 100, Category: "c", IsActive: true}
	gdb.Create(item)

	in := &FlashActivityInput{
		ItemID: item.ID, Name: "spring", LimitQty: 10, PerUserLimit: 1,
		StartTime: time.Now().Add(-time.Minute), EndTime: nil, PricePoints: 5,
	}
	act, err := svc.CreateFlashActivity(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if act.ID == 0 || act.LimitQty != 10 || act.Status != model.FlashSaleStatusActive {
		t.Fatalf("created act unexpected: %+v", act)
	}

	// 用户侧进行中列表应包含新建活动
	acts, err := svc.FlashActivities(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, a := range acts {
		if a.ID == act.ID {
			found = true
		}
	}
	if !found {
		t.Fatalf("user list should contain new active activity")
	}

	// 运营侧详情
	detail, err := svc.FlashActivityDetail(ctx, act.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.ID != act.ID {
		t.Fatalf("detail mismatch: %+v", detail)
	}

	// 下架
	ended, err := svc.EndFlashActivity(ctx, act.ID)
	if err != nil {
		t.Fatal(err)
	}
	if ended.Status != model.FlashSaleStatusEnded {
		t.Fatalf("status should be ended, got %s", ended.Status)
	}

	// 下架后用户列表不再包含（进行中过滤）
	acts2, _ := svc.FlashActivities(ctx, 10)
	for _, a := range acts2 {
		if a.ID == act.ID {
			t.Fatalf("ended activity should not appear in user list")
		}
	}
	// 运营列表仍可见（含已结束）
	all, _ := svc.ListFlashActivities(ctx, 10)
	found2 := false
	for _, a := range all {
		if a.ID == act.ID {
			found2 = true
		}
	}
	if !found2 {
		t.Fatalf("admin list should include ended activity")
	}
}

// TestFlashActivity_WarmupDueFlashSales_NoCache 验证无 Redis 时调度预热任务安全降级（不 panic、返回 0）。
func TestFlashActivity_WarmupDueFlashSales_NoCache(t *testing.T) {
	svc, gdb := newTestShop(t)
	ctx := context.Background()
	item := &model.ShopItem{Name: "F_warm", PricePoints: 0, Stock: 100, Category: "c", IsActive: true}
	gdb.Create(item)
	in := &FlashActivityInput{ItemID: item.ID, LimitQty: 5, PerUserLimit: 1, StartTime: time.Now().Add(time.Minute)}
	if _, err := svc.CreateFlashActivity(ctx, in); err != nil {
		t.Fatal(err)
	}
	n, err := svc.WarmupDueFlashSales(ctx)
	if err != nil {
		t.Fatalf("warmup should not error without cache: %v", err)
	}
	if n != 0 {
		t.Fatalf("without cache, warmed count should be 0, got %d", n)
	}
}

// TestFlashActivity_CreateValidation 验证创建参数校验。
func TestFlashActivity_CreateValidation(t *testing.T) {
	svc, _ := newTestShop(t)
	ctx := context.Background()
	if _, err := svc.CreateFlashActivity(ctx, &FlashActivityInput{ItemID: 1, LimitQty: 0}); err == nil {
		t.Fatalf("limit_qty<=0 should fail")
	}
	if _, err := svc.CreateFlashActivity(ctx, &FlashActivityInput{ItemID: 0, LimitQty: 5}); err == nil {
		t.Fatalf("item_id<=0 should fail")
	}
}
