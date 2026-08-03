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

// RedeemLogic 积分兑换（扣库存 → 扣积分 → 建单，失败回滚补偿）
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

	// 商品在售校验
	var item model.ShopItem
	err := l.svcCtx.Db.Where("id = ? AND is_active = ?", in.ItemId, true).First(&item).Error
	if err == gorm.ErrRecordNotFound {
		return nil, errorx.New(errorx.CodeItemOffline)
	}
	if err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}
	cost := item.PricePoints * int64(qty)

	// 原子扣库存（WHERE stock >= qty 行锁串行，防超卖）
	result := l.svcCtx.Db.Model(&model.ShopItem{}).
		Where("id = ? AND is_active = ? AND stock >= ?", in.ItemId, true, qty).
		UpdateColumn("stock", gorm.Expr("stock - ?", qty))
	if result.Error != nil {
		return nil, errorx.New(errorx.CodeDBError, result.Error.Error())
	}
	if result.RowsAffected == 0 {
		return nil, errorx.New(errorx.CodeStockInsufficient)
	}

	// 扣积分（user-rpc）；失败回滚库存
	_, err = l.svcCtx.UserRpc.DeductPoints(l.ctx, &userpb.DeductPointsRequest{
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
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}

	// 兑换进度上报（失败不阻塞主流程）
	l.reportRedeemProgress(in.UserId)

	return &shop.RedeemResponse{Order: toOrderInfo(order)}, nil
}

func (l *RedeemLogic) reportRedeemProgress(userID string) {
	for _, key := range []string{
		model.TaskKeyDailyRedeem1,
		model.TaskKeyWeeklyRedeem3,
		model.TaskKeyAchieveRedeem100,
	} {
		if _, err := l.svcCtx.TaskRpc.ReportProgress(l.ctx, &taskpb.ReportProgressRequest{
			UserId:  userID,
			TaskKey: key,
			Delta:   1,
		}); err != nil {
			l.Logger.Errorf("report task progress %s failed: %v", key, err)
		}
	}
}
