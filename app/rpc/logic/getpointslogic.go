package logic

import (
	"context"

	"github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/app/rpc/svc"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/gormx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
)

// GetPointsLogic 查询积分余额
type GetPointsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetPointsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetPointsLogic {
	return &GetPointsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetPointsLogic) GetPoints(in *hc.GetPointsRequest) (*hc.GetPointsResponse, error) {
	var u model.User
	err := l.svcCtx.Db.Where("user_id = ?", in.UserId).First(&u).Error
	if gormx.IsRecordNotFound(err) {
		return nil, errorx.New(errorx.CodeNotFound, "user not found")
	}
	if err != nil {
		return nil, errorx.NewErr(errorx.CodeDBError, err)
	}
	return &hc.GetPointsResponse{
		UserId:        u.UserID,
		PointsBalance: u.PointsBalance,
	}, nil
}
