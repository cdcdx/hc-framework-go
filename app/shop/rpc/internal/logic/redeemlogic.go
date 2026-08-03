package logic

import (
	"context"

	"github.com/cdcdx/hc-framework-go/app/shop/rpc/internal/svc"
	"github.com/cdcdx/hc-framework-go/app/shop/rpc/shop"
	taskpb "github.com/cdcdx/hc-framework-go/app/task/rpc/task"
	userpb "github.com/cdcdx/hc-framework-go/app/user/rpc/user"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

// RedeemLogic 普通积分兑换（扣库存防超卖 + 扣积分 + 建单）
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

func (l *RedeemLogic) Redeem(in *shop.RedeemRequest) (*shop.RedeemResponse, error) {
	qty := int(in.Quantity)
	if qty <= 0 {
		qty = 1
	}

	var item model.ShopItem
	err := l.svcCtx.Db.Where("id = ?", in.ItemId).First(&item).Error
	if err == gorm.ErrRecordNotFound {
		return nil, errorx.New(errorx.CodeNotFound, "item not found")
	}
	if err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}
	if !item.IsActive {
		return nil, errorx.New(errorx.CodeNotFound, "item not active")
	}

	// 原子扣库存（防超卖）
	result := l.svcCtx.Db.Model(&model.ShopItem{}).
		Where("id = ? AND stock >= ?", item.ID, qty).
		UpdateColumn("stock", gorm.Expr("stock - ?", qty))
	if result.Error != nil {
		return nil, errorx.New(errorx.CodeDBError, result.Error.Error())
	}
	if result.RowsAffected == 0 {
		return nil, errorx.New(errorx.CodeStockInsufficient)
	}

	total := item.PricePoints * int64(qty)
	// 扣积分；失败回滚库存
	if _, err := l.svcCtx.UserRpc.DeductPoints(l.ctx, &userpb.DeductPointsRequest{
		UserId: in.UserId,
		Points: total,
		Reason: "redeem",
	}); err != nil {
		if rb := l.svcCtx.Db.Model(&model.ShopItem{}).
			Where("id = ?", item.ID).
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
		PointsSpent: total,
		OrderStatus: "completed",
		ActivityID:  0,
	}
	if err := l.svcCtx.Db.Create(order).Error; err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}

	// 兑换进度上报（失败不阻塞主流程）
	for _, key := range []string{
		model.TaskKeyDailyRedeem1,
		model.TaskKeyWeeklyRedeem3,
		model.TaskKeyAchieveRedeem100,
	} {
		if _, err := l.svcCtx.TaskRpc.ReportProgress(l.ctx, &taskpb.ReportProgressRequest{
			UserId:  in.UserId,
			TaskKey: key,
			Delta:   1,
		}); err != nil {
			l.Logger.Errorf("report task progress %s failed: %v", key, err)
		}
	}

	return &shop.RedeemResponse{Order: toOrderInfo(order)}, nil
}
