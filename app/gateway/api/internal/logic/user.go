package logic

import (
	"context"

	"github.com/cdcdx/hc-framework-go/app/gateway/api/internal/svc"
	"github.com/cdcdx/hc-framework-go/app/gateway/api/internal/types"
	hcpb "github.com/cdcdx/hc-framework-go/app/rpc/hc"
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
	resp, err := l.svcCtx.HcRpc.GetProfile(l.ctx, &hcpb.GetProfileRequest{UserId: uid})
	if err != nil {
		return nil, err
	}
	// 解包专用响应，保持 HTTP data 直接为用户对象（与 gin 版契约一致）
	return toData(resp.User), nil
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
	resp, err := l.svcCtx.HcRpc.UpdateProfile(l.ctx, &hcpb.UpdateProfileRequest{
		UserId:    uid,
		Username:  req.Username,
		AvatarUrl: req.AvatarURL,
	})
	if err != nil {
		return nil, err
	}
	// 解包专用响应，保持 HTTP data 直接为用户对象（与 gin 版契约一致）
	return toData(resp.User), nil
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
	resp, err := l.svcCtx.HcRpc.GetPoints(l.ctx, &hcpb.GetPointsRequest{UserId: uid})
	if err != nil {
		return nil, err
	}
	return toData(resp), nil
}
