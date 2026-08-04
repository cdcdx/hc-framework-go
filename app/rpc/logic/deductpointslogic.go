package logic

import (
	"context"

	"github.com/cdcdx/hc-framework-go/app/rpc/svc"
	"github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

// DeductPointsLogic 积分扣减（兑换扣款，余额不足报错；原子条件更新防负余额）
type DeductPointsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewDeductPointsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *DeductPointsLogic {
	return &DeductPointsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *DeductPointsLogic) DeductPoints(in *hc.DeductPointsRequest) (*hc.DeductPointsResponse, error) {
	if in.Points <= 0 {
		return nil, errorx.New(errorx.CodeInvalidParam, "points must be positive")
	}

	// 原子条件扣减：余额充足才扣，杜绝并发负余额
	result := l.svcCtx.Db.Model(&model.User{}).
		Where("user_id = ? AND points_balance >= ?", in.UserId, in.Points).
		UpdateColumn("points_balance", gorm.Expr("points_balance - ?", in.Points))
	if result.Error != nil {
		return nil, errorx.New(errorx.CodeDBError, result.Error.Error())
	}
	if result.RowsAffected == 0 {
		// 区分「用户不存在」与「余额不足」
		var count int64
		if err := l.svcCtx.Db.Model(&model.User{}).Where("user_id = ?", in.UserId).Count(&count).Error; err != nil {
			return nil, errorx.New(errorx.CodeDBError, err.Error())
		}
		if count == 0 {
			return nil, errorx.New(errorx.CodeNotFound, "user not found")
		}
		return nil, errorx.New(errorx.CodePointsInsufficient)
	}

	var balance int64
	if err := l.svcCtx.Db.Model(&model.User{}).
		Where("user_id = ?", in.UserId).
		Pluck("points_balance", &balance).Error; err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}
	return &hc.DeductPointsResponse{UserId: in.UserId, PointsBalance: balance}, nil
}
