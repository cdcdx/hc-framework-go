package logic

import (
	"context"

	"github.com/cdcdx/hc-framework-go/app/user/rpc/internal/svc"
	"github.com/cdcdx/hc-framework-go/app/user/rpc/user"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
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

func (l *GetPointsLogic) GetPoints(in *user.GetPointsRequest) (*user.GetPointsResponse, error) {
	var u model.User
	err := l.svcCtx.Db.Where("user_id = ?", in.UserId).First(&u).Error
	if err == gorm.ErrRecordNotFound {
		return nil, errorx.New(errorx.CodeNotFound, "user not found")
	}
	if err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}
	return &user.GetPointsResponse{
		UserId:        u.UserID,
		PointsBalance: u.PointsBalance,
	}, nil
}
