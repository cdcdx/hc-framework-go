package logic

import (
	"context"

	"github.com/cdcdx/hc-framework-go/app/gateway/api/internal/svc"
	"github.com/cdcdx/hc-framework-go/app/gateway/api/internal/types"
	userpb "github.com/cdcdx/hc-framework-go/app/user/rpc/user"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/zeromicro/go-zero/core/logx"
)

// errUserUnauthorized 未从上下文取到 user_id（中间件失效或未挂载）
var errUserUnauthorized = errorx.New(errorx.CodeTokenInvalid, "unauthorized")

// GetProfileLogic 用户资料
type GetProfileLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetProfileLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetProfileLogic {
	return &GetProfileLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetProfileLogic) GetProfile() (any, error) {
	uid, ok := requireUser(l.ctx)
	if !ok {
		return nil, errUserUnauthorized
	}
	resp, err := l.svcCtx.UserRpc.GetProfile(l.ctx, &userpb.GetProfileRequest{UserId: uid})
	if err != nil {
		return nil, err
	}
	return toData(resp), nil
}

// UpdateProfileLogic 更新用户资料
type UpdateProfileLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateProfileLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateProfileLogic {
	return &UpdateProfileLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *UpdateProfileLogic) UpdateProfile(req *types.UpdateProfileReq) (any, error) {
	uid, ok := requireUser(l.ctx)
	if !ok {
		return nil, errUserUnauthorized
	}
	resp, err := l.svcCtx.UserRpc.UpdateProfile(l.ctx, &userpb.UpdateProfileRequest{
		UserId:    uid,
		Username:  req.Username,
		AvatarUrl: req.AvatarURL,
	})
	if err != nil {
		return nil, err
	}
	return toData(resp), nil
}

// GetPointsLogic 积分余额
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

func (l *GetPointsLogic) GetPoints() (any, error) {
	uid, ok := requireUser(l.ctx)
	if !ok {
		return nil, errUserUnauthorized
	}
	resp, err := l.svcCtx.UserRpc.GetPoints(l.ctx, &userpb.GetPointsRequest{UserId: uid})
	if err != nil {
		return nil, err
	}
	return toData(resp), nil
}
