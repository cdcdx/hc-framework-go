package repository

import (
	"context"
	"fmt"
	"testing"

	"github.com/cdcdx/hc-framework-go/internal/db"
	"github.com/cdcdx/hc-framework-go/internal/model"
)

// TestShopRepo_FindItems_FilterAndPagination 验证分类筛选与游标分页（limit+1 探测 hasMore）。
func TestShopRepo_FindItems_FilterAndPagination(t *testing.T) {
	gdb := openRepoDB(t, &model.ShopItem{}, &model.RedeemOrder{}, &model.PointsTransaction{}, &model.ShopItemStockBucket{})
	repo := NewShopRepositoryWithCache(db.CreateSingleRWDB(gdb), nil)

	create := func(name, cat string, stock int) {
		if err := gdb.Create(&model.ShopItem{Name: name, Category: cat, Stock: stock, PricePoints: 10, Version: 0, IsActive: true}).Error; err != nil {
			t.Fatal(err)
		}
	}
	create("a1", "catA", 5)
	create("a2", "catA", 5)
	create("b1", "catB", 5)

	// 全量：3 个
	all, err := repo.FindItems(context.Background(), 0, 20, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("FindItems('') = %d, want 3", len(all))
	}
	// 分类筛选
	catA, err := repo.FindItems(context.Background(), 0, 20, "catA")
	if err != nil {
		t.Fatal(err)
	}
	if len(catA) != 2 {
		t.Fatalf("FindItems(catA) = %d, want 2", len(catA))
	}
	// 下架商品不出现（gorm default:true 会使显式 false 不落库，故用 UpdateColumn 强制置否）
	off := &model.ShopItem{Name: "off", Category: "catA", Stock: 5, PricePoints: 10, Version: 0, IsActive: false}
	gdb.Create(off)
	gdb.Model(&model.ShopItem{}).Where("id = ?", off.ID).UpdateColumn("is_active", false)
	catA2, _ := repo.FindItems(context.Background(), 0, 20, "catA")
	if len(catA2) != 2 {
		t.Fatalf("inactive item must be excluded, got %d want 2", len(catA2))
	}
}

// TestShopRepo_FindItemByID 验证按 ID 查询与不存在返回 nil。
func TestShopRepo_FindItemByID(t *testing.T) {
	gdb := openRepoDB(t, &model.ShopItem{}, &model.RedeemOrder{}, &model.PointsTransaction{}, &model.ShopItemStockBucket{})
	repo := NewShopRepositoryWithCache(db.CreateSingleRWDB(gdb), nil)
	item := &model.ShopItem{Name: "x", Category: "c", Stock: 1, PricePoints: 10, Version: 0, IsActive: true}
	gdb.Create(item)

	got, err := repo.FindItemByID(context.Background(), item.ID)
	if err != nil || got == nil || got.ID != item.ID {
		t.Fatalf("FindItemByID = %+v, err=%v", got, err)
	}
	missing, err := repo.FindItemByID(context.Background(), 999999)
	if err != nil {
		t.Fatal(err)
	}
	if missing != nil {
		t.Fatalf("FindItemByID(missing) = %+v, want nil", missing)
	}
}

// TestShopRepo_FindOrders_Pagination 验证按用户分页（id DESC）。
func TestShopRepo_FindOrders_Pagination(t *testing.T) {
	gdb := openRepoDB(t, &model.ShopItem{}, &model.RedeemOrder{}, &model.PointsTransaction{}, &model.ShopItemStockBucket{})
	repo := NewShopRepositoryWithCache(db.CreateSingleRWDB(gdb), nil)

	for i := 0; i < 3; i++ {
		gdb.Create(&model.RedeemOrder{UserID: "u1", ItemID: 1, ItemName: "x", PointsSpent: 10, OrderStatus: "completed"})
	}
	// 其他用户的订单不应混入
	gdb.Create(&model.RedeemOrder{UserID: "u2", ItemID: 1, ItemName: "x", PointsSpent: 10, OrderStatus: "completed"})

	orders, err := repo.FindOrders(context.Background(), "u1", 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(orders) != 3 {
		t.Fatalf("FindOrders(u1) = %d, want 3 (limit+1 探测)", len(orders))
	}
	// 默认按 id DESC：第一条订单 id 最大
	if len(orders) >= 2 && orders[0].ID < orders[1].ID {
		t.Fatal("FindOrders should be ordered by id DESC")
	}
}

// TestShopRepo_DeductStock 验证原子条件扣减：库存充足才扣减，库存不足则失败（防超卖）。
func TestShopRepo_DeductStock(t *testing.T) {
	gdb := openRepoDB(t, &model.ShopItem{}, &model.RedeemOrder{}, &model.PointsTransaction{}, &model.ShopItemStockBucket{})
	repo := NewShopRepositoryWithCache(db.CreateSingleRWDB(gdb), nil)
	item := &model.ShopItem{Name: "x", Category: "c", Stock: 10, PricePoints: 10, Version: 0, IsActive: true}
	gdb.Create(item)

	if err := repo.DeductStock(context.Background(), item.ID, 3, "u1"); err != nil {
		t.Fatalf("DeductStock: %v", err)
	}
	var updated model.ShopItem
	gdb.First(&updated, item.ID)
	if updated.Stock != 7 {
		t.Fatalf("after deduct: stock=%d, want 7", updated.Stock)
	}

	// 库存不足再扣（剩余 7，扣 10）：RowsAffected=0 → 错误，且不改变库存
	if err := repo.DeductStock(context.Background(), item.ID, 10, "u1"); err == nil {
		t.Fatal("expected error when stock insufficient")
	}
	var after model.ShopItem
	gdb.First(&after, item.ID)
	if after.Stock != 7 {
		t.Fatalf("stock should not change on insufficient deduct: got %d", after.Stock)
	}
}

// TestShopRepo_DeductStock_Buckets 验证分桶扣减（P2-5）：
// 16 库存均分到 16 个桶（每桶 1），顺序扣 16 次（每桶 1）后全桶耗尽，第 17 次必须失败（零超卖）；
// 且分桶路径不触碰 shop_items 单行（raw stock 列保持 16，权威值来自桶之和）。
func TestShopRepo_DeductStock_Buckets(t *testing.T) {
	gdb := openRepoDB(t, &model.ShopItem{}, &model.RedeemOrder{}, &model.PointsTransaction{}, &model.ShopItemStockBucket{})
	repo := NewShopRepositoryWithCache(db.CreateSingleRWDB(gdb), nil)
	item := &model.ShopItem{Name: "xb", Category: "c", Stock: 16, PricePoints: 10, Version: 0, IsActive: true}
	gdb.Create(item)

	if err := repo.CreateStockBuckets(context.Background(), item.ID, 16); err != nil {
		t.Fatal(err)
	}
	total, err := repo.TotalStock(context.Background(), item.ID)
	if err != nil || total != 16 {
		t.Fatalf("TotalStock = %d, err=%v, want 16", total, err)
	}

	for i := 0; i < 16; i++ {
		if err := repo.DeductStock(context.Background(), item.ID, 1, fmt.Sprintf("u_%d", i)); err != nil {
			t.Fatalf("deduct %d: %v", i, err)
		}
	}
	total, _ = repo.TotalStock(context.Background(), item.ID)
	if total != 0 {
		t.Fatalf("after 16 deducts total=%d, want 0", total)
	}
	// 全桶耗尽 → 错误（不超卖）
	if err := repo.DeductStock(context.Background(), item.ID, 1, "u_over"); err == nil {
		t.Fatal("expected error when all buckets exhausted")
	}
	// 分桶路径未触碰 shop_items.stock 热点列
	var raw model.ShopItem
	gdb.First(&raw, item.ID)
	if raw.Stock != 16 {
		t.Fatalf("bucket path must not mutate shop_items.stock, got %d want 16", raw.Stock)
	}
}

// TestShopRepo_BackfillStockBuckets 验证存量回填：商品有库存但无桶时，回填后桶之和 == 原 stock。
func TestShopRepo_BackfillStockBuckets(t *testing.T) {
	gdb := openRepoDB(t, &model.ShopItem{}, &model.RedeemOrder{}, &model.PointsTransaction{}, &model.ShopItemStockBucket{})
	repo := NewShopRepositoryWithCache(db.CreateSingleRWDB(gdb), nil)
	item := &model.ShopItem{Name: "bf", Category: "c", Stock: 40, PricePoints: 10, Version: 0, IsActive: true}
	gdb.Create(item)

	if err := repo.BackfillStockBuckets(context.Background()); err != nil {
		t.Fatal(err)
	}
	total, err := repo.TotalStock(context.Background(), item.ID)
	if err != nil || total != 40 {
		t.Fatalf("after backfill TotalStock=%d err=%v, want 40", total, err)
	}
	// 幂等：再次回填不应改变桶之和
	if err := repo.BackfillStockBuckets(context.Background()); err != nil {
		t.Fatal(err)
	}
	total, _ = repo.TotalStock(context.Background(), item.ID)
	if total != 40 {
		t.Fatalf("idempotent backfill changed stock: %d", total)
	}
}
