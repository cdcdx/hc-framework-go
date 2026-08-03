package logic

import (
	"context"
	"time"

	"github.com/cdcdx/hc-framework-go/app/shop/rpc/internal/svc"
	"github.com/cdcdx/hc-framework-go/app/shop/rpc/shop"
	taskpb "github.com/cdcdx/hc-framework-go/app/task/rpc/task"
	userpb "github.com/cdcdx/hc-framework-go/app/user/rpc/user"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

// FlashRedeemLogic 定时抢购（限量内入库，超出直接拒绝；原子抢配额防超卖）
type FlashRedeemLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewFlashRedeemLogic(ctx context.Context, svcCtx *svc.ServiceContext) *FlashRedeemLogic {
	return &FlashRedeemLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *FlashRedeemLogic) FlashRedeem(in *shop.FlashRedeemRequest) (*shop.RedeemResponse, error) {
	var act model.ShopFlashActivity
	err := l.svcCtx.Db.Where("id = ?", in.ActivityId).First(&act).Error
	if err == gorm.ErrRecordNotFound {
		return nil, errorx.New(errorx.CodeNotFound, "flash sale activity not found")
	}
	if err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}

	// 时间窗与状态校验
	now := time.Now()
	if act.Status == model.FlashSaleStatusEnded ||
		(act.EndTime != nil && now.After(*act.EndTime)) {
		return nil, errorx.New(errorx.CodeFlashSaleEnded)
	}
	if now.Before(act.StartTime) {
		return nil, errorx.New(errorx.CodeFlashSaleNotStarted)
	}
	if act.SoldQty >= act.LimitQty {
		return nil, errorx.New(errorx.CodeFlashSaleSoldOut)
	}

	// 每人限购（DB 权威校验：该用户对活动的订单数）
	perUser := act.PerUserLimit
	if perUser <= 0 {
		perUser = 1
	}
	var cnt int64
	if err := l.svcCtx.Db.Model(&model.RedeemOrder{}).
		Where("user_id = ? AND activity_id = ?", in.UserId, act.ID).
		Count(&cnt).Error; err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}
	if cnt >= int64(perUser) {
		return nil, errorx.New(errorx.CodeFlashSaleUserLimit)
	}

	// 原子抢配额（WHERE sold_qty < limit_qty 行锁串行，绝不超卖）
	result := l.svcCtx.Db.Model(&model.ShopFlashActivity{}).
		Where("id = ? AND sold_qty < limit_qty", act.ID).
		UpdateColumn("sold_qty", gorm.Expr("sold_qty + 1"))
	if result.Error != nil {
		return nil, errorx.New(errorx.CodeDBError, result.Error.Error())
	}
	if result.RowsAffected == 0 {
		return nil, errorx.New(errorx.CodeFlashSaleSoldOut)
	}

	// 扣积分（抢购价，user-rpc）；失败回滚配额
	_, err = l.svcCtx.UserRpc.DeductPoints(l.ctx, &userpb.DeductPointsRequest{
		UserId: in.UserId,
		Points: act.PricePoints,
		Reason: "flash",
	})
	if err != nil {
		if rb := l.svcCtx.Db.Model(&model.ShopFlashActivity{}).
			Where("id = ?", act.ID).
			UpdateColumn("sold_qty", gorm.Expr("sold_qty - 1")); rb.Error != nil {
			l.Logger.Errorf("rollback flash quota failed: %v", rb.Error)
		}
		return nil, err
	}

	// 建单（关联 activity_id）
	order := &model.RedeemOrder{
		UserID:      in.UserId,
		ItemID:      act.ItemID,
		ItemName:    act.Name,
		PointsSpent: act.PricePoints,
		OrderStatus: "completed",
		ActivityID:  act.ID,
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
