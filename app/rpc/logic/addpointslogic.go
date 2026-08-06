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

	// 余额 = 更新前查询值 + 增量（原子更新已在 DB 完成，此处仅用于返回，省去一次 Pluck 查询）。
	// 并发下可能略旧，但客户端下次刷新可拿到最新值；如需精确可改走 Pluck。
	newBalance := u.PointsBalance + in.Points
	// 余额已变更，失效缓存（短 TTL 兜底，这里主动失效保证读一致性）
	if l.svcCtx.Cache != nil {
		_ = l.svcCtx.Cache.Delete(l.ctx, pointsBalanceKey(in.UserId))
	}
	return &hc.AddPointsResponse{UserId: in.UserId, PointsBalance: newBalance}, nil
}
