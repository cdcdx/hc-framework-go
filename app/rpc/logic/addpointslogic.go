package logic

import (
	"context"

	"github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/app/rpc/svc"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/gormx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

// AddPointsLogic 积分入账（挂机结算 / 任务奖励，负值非法）
type AddPointsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewAddPointsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *AddPointsLogic {
	return &AddPointsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *AddPointsLogic) AddPoints(in *hc.AddPointsRequest) (*hc.AddPointsResponse, error) {
	if in.Points <= 0 {
		return nil, errorx.New(errorx.CodeInvalidParam, "points must be positive")
	}

	var u model.User
	err := l.svcCtx.Db.Where("user_id = ?", in.UserId).First(&u).Error
	if gormx.IsRecordNotFound(err) {
		return nil, errorx.New(errorx.CodeNotFound, "user not found")
	}
	if err != nil {
		return nil, errorx.NewErr(errorx.CodeDBError, err)
	}
	if u.Status != "active" {
		return nil, errorx.New(errorx.CodeAccountLocked)
	}

	if err := l.svcCtx.Db.Model(&model.User{}).
		Where("user_id = ?", in.UserId).
		UpdateColumn("points_balance", gorm.Expr("points_balance + ?", in.Points)).Error; err != nil {
		return nil, errorx.NewErr(errorx.CodeDBError, err)
	}

	var balance int64
	if err := l.svcCtx.Db.Model(&model.User{}).
		Where("user_id = ?", in.UserId).
		Pluck("points_balance", &balance).Error; err != nil {
		return nil, errorx.NewErr(errorx.CodeDBError, err)
	}
	return &hc.AddPointsResponse{UserId: in.UserId, PointsBalance: balance}, nil
}
