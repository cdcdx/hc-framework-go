package logic

import (
	"context"
	"testing"

	"github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/app/rpc/svc"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/model"
)

// setupShopTestCtx 构建带商品/订单表的测试上下文。
func setupShopTestCtx(t *testing.T) *svc.ServiceContext {
	t.Helper()
	svcCtx := newTestSvc(t)
	if err := svcCtx.Db.AutoMigrate(&model.ShopItem{}, &model.RedeemOrder{}); err != nil {
		t.Fatalf("automigrate shop: %v", err)
	}
	return svcCtx
}

func seedItem(t *testing.T, svcCtx *svc.ServiceContext, stock int, price int64, active bool) int64 {
	t.Helper()
	item := model.ShopItem{
		Name:        "test-item",
		PricePoints: price,
		Stock:       stock,
		IsActive:    true, // 先以默认值写入（GORM 的 default:true 会忽略 false 零值）
	}
	if err := svcCtx.Db.Create(&item).Error; err != nil {
		t.Fatalf("seed item: %v", err)
	}
	// 显式覆盖下架状态，绕开 default 标签对零值的处理。
	if !active {
		if err := svcCtx.Db.Model(&item).Update("is_active", false).Error; err != nil {
			t.Fatalf("set item offline: %v", err)
		}
	}
	return item.ID
}

func TestRedeem_Success(t *testing.T) {
	ctx := context.Background()
	svcCtx := setupShopTestCtx(t)
	seedUser(t, svcCtx, "u1", 1000)
	itemID := seedItem(t, svcCtx, 10, 100, true)

	resp, err := NewRedeemLogic(ctx, svcCtx).Redeem(&hc.ShopRedeemRequest{
		UserId:   "u1",
		ItemId:   itemID,
		Quantity: 2,
	})
	if err != nil {
		t.Fatalf("redeem: %v", err)
	}

	// 扣库存 10 - 2 = 8
	var item model.ShopItem
	if err := svcCtx.Db.First(&item, itemID).Error; err != nil {
		t.Fatal(err)
	}
	if item.Stock != 8 {
		t.Fatalf("stock expected 8, got %d", item.Stock)
	}

	// 扣积分 1000 - 200 = 800
	var u model.User
	if err := svcCtx.Db.Where("user_id = ?", "u1").First(&u).Error; err != nil {
		t.Fatal(err)
	}
	if u.PointsBalance != 800 {
		t.Fatalf("balance expected 800, got %d", u.PointsBalance)
	}

	// 建单
	if resp.Order == nil || resp.Order.PointsSpent != 200 {
		t.Fatalf("order points expected 200, got %+v", resp.Order)
	}
}

func TestRedeem_StockInsufficientRollback(t *testing.T) {
	ctx := context.Background()
	svcCtx := setupShopTestCtx(t)
	seedUser(t, svcCtx, "u1", 1000)
	itemID := seedItem(t, svcCtx, 1, 100, true)

	// 数量 5 > 库存 1，应报库存不足，且积分不被扣（回滚）
	_, err := NewRedeemLogic(ctx, svcCtx).Redeem(&hc.ShopRedeemRequest{
		UserId:   "u1",
		ItemId:   itemID,
		Quantity: 5,
	})
	if errorx.Code(err) != errorx.CodeStockInsufficient {
		t.Fatalf("expected CodeStockInsufficient, got %d (%v)", errorx.Code(err), err)
	}

	var u model.User
	_ = svcCtx.Db.Where("user_id = ?", "u1").First(&u).Error
	if u.PointsBalance != 1000 {
		t.Fatalf("balance should remain 1000 after rollback, got %d", u.PointsBalance)
	}
}

func TestRedeem_ItemOffline(t *testing.T) {
	ctx := context.Background()
	svcCtx := setupShopTestCtx(t)
	seedUser(t, svcCtx, "u1", 1000)
	itemID := seedItem(t, svcCtx, 10, 100, false) // 下架

	_, err := NewRedeemLogic(ctx, svcCtx).Redeem(&hc.ShopRedeemRequest{
		UserId:   "u1",
		ItemId:   itemID,
		Quantity: 1,
	})
	// 下架商品不可兑换：不扣库存、返回非成功错误码（热点路径下不额外查询区分下线/售罄）。
	if err == nil {
		t.Fatal("expected error for offline item")
	}
	if errorx.Code(err) != errorx.CodeStockInsufficient {
		t.Fatalf("expected CodeStockInsufficient, got %d (%v)", errorx.Code(err), err)
	}
	// 库存未被误扣
	var item model.ShopItem
	_ = svcCtx.Db.First(&item, itemID).Error
	if item.Stock != 10 {
		t.Fatalf("offline item stock should remain 10, got %d", item.Stock)
	}
}

func TestRedeem_PointsInsufficient(t *testing.T) {
	ctx := context.Background()
	svcCtx := setupShopTestCtx(t)
	seedUser(t, svcCtx, "u1", 50) // 余额不足
	itemID := seedItem(t, svcCtx, 10, 100, true)

	_, err := NewRedeemLogic(ctx, svcCtx).Redeem(&hc.ShopRedeemRequest{
		UserId:   "u1",
		ItemId:   itemID,
		Quantity: 1, // 需要 100 积分，余额 50
	})
	if errorx.Code(err) != errorx.CodePointsInsufficient {
		t.Fatalf("expected CodePointsInsufficient, got %d (%v)", errorx.Code(err), err)
	}

	// 扣积分失败后库存应回滚（保持 10）
	var item model.ShopItem
	_ = svcCtx.Db.First(&item, itemID).Error
	if item.Stock != 10 {
		t.Fatalf("stock should remain 10 after points rollback, got %d", item.Stock)
	}
}
