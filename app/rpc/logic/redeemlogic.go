package logic

import (
	"context"

	"github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/app/rpc/svc"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

// RedeemLogic 积分兑换（扣库存 → 扣积分 → 建单，失败回滚补偿）
// 合并为单 rpc 后，扣积分与进度上报均为进程内调用。
type RedeemLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewRedeemLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RedeemLogic {
	return &RedeemLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *RedeemLogic) Redeem(in *hc.ShopRedeemRequest) (*hc.RedeemResponse, error) {
	qty := int(in.Quantity)
	if qty <= 0 {
		qty = 1
	}

	// 先原子扣库存（WHERE stock >= qty 行锁串行，防超卖）。这是唯一持锁点，
	// 且成功路径无需任何前置 SELECT，避免在热点行上每请求多一次查询放大连接池等待。
	result := l.svcCtx.Db.Model(&model.ShopItem{}).
		Where("id = ? AND is_active = ? AND stock >= ?", in.ItemId, true, qty).
		UpdateColumn("stock", gorm.Expr("stock - ?", qty))
	if result.Error != nil {
		return nil, errorx.NewErr(errorx.CodeDBError, result.Error)
	}
	if result.RowsAffected == 0 {
		// 扣库存失败（库存不足或商品不可用）。秒杀热点场景下此分支占比极高，
		// 为避免每次失败再发一次 SELECT 查询放大慢 SQL，统一返回库存不足；
		// 商品下架对用户而言同样表现为"暂不可兑换"，无需额外区分。
		return nil, errorx.New(errorx.CodeStockInsufficient)
	}

	// 扣库存成功后，仅查建单所需字段（快照读，不持锁）。
	var item struct {
		ID          int64  `gorm:"column:id"`
		Name        string `gorm:"column:name"`
		PricePoints int64  `gorm:"column:price_points"`
	}
	if err := l.svcCtx.Db.Model(&model.ShopItem{}).
		Select("id", "name", "price_points").
		Where("id = ?", in.ItemId).
		First(&item).Error; err != nil {
		return nil, errorx.NewErr(errorx.CodeDBError, err)
	}
	cost := item.PricePoints * int64(qty)

	// 扣积分（进程内 user 域）；失败回滚库存
	_, err := NewDeductPointsLogic(l.ctx, l.svcCtx).DeductPoints(&hc.DeductPointsRequest{
		UserId: in.UserId,
		Points: cost,
		Reason: "redeem",
	})
	if err != nil {
		if rb := l.svcCtx.Db.Model(&model.ShopItem{}).
			Where("id = ?", in.ItemId).
			UpdateColumn("stock", gorm.Expr("stock + ?", qty)); rb.Error != nil {
			l.Logger.Errorf("rollback stock failed: %v", rb.Error)
		}
		return nil, err
	}

	// 建单
	order := &model.RedeemOrder{
		UserID:      in.UserId,
		ItemID:      item.ID,
		ItemName:    item.Name,
		PointsSpent: cost,
		OrderStatus: "completed",
		ActivityID:  0,
	}
	if err := l.svcCtx.Db.Create(order).Error; err != nil {
		return nil, errorx.NewErr(errorx.CodeDBError, err)
	}

	// 兑换进度上报（进程内 task 域，失败不阻塞主流程）
	reportRedeemProgress(l.ctx, l.svcCtx, in.UserId, l.Logger)

	return &hc.RedeemResponse{Order: toOrderInfo(order)}, nil
}
