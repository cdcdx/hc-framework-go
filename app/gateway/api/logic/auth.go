package logic

import (
	"context"

	"github.com/cdcdx/hc-framework-go/app/gateway/api/svc"
	"github.com/cdcdx/hc-framework-go/app/gateway/api/types"
	hcpb "github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
)

// RegisterLogic 邮箱注册
type RegisterLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewRegisterLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RegisterLogic {
	return &RegisterLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *RegisterLogic) Register(req *types.RegisterReq) (any, error) {
	resp, err := l.svcCtx.HcRpc.Register(l.ctx, &hcpb.RegisterRequest{
		Email:    req.Email,
		Password: req.Password,
		Username: req.Username,
	})
	if err != nil {
		return nil, err
	}
	return toData(resp), nil
}

// LoginLogic 邮箱登录（成功后上报每日登录任务进度）
type LoginLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewLoginLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LoginLogic {
	return &LoginLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *LoginLogic) Login(req *types.LoginReq) (any, error) {
	resp, err := l.svcCtx.HcRpc.Login(l.ctx, &hcpb.LoginRequest{
		Email:    req.Email,
		Password: req.Password,
	})
	if err != nil {
		return nil, err
	}

	// 每日登录任务（失败不阻塞登录）
	if resp.User != nil {
		if _, err := l.svcCtx.HcRpc.ReportProgress(l.ctx, &hcpb.ReportProgressRequest{
			UserId:  resp.User.UserId,
			TaskKey: model.TaskKeyDailyLogin,
			Delta:   1,
		}); err != nil {
			l.Logger.Errorf("report daily_login progress failed: %v", err)
		}
	}

	return toData(resp), nil
}

// GoogleOAuthLogic Google OAuth 登录/注册
type GoogleOAuthLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGoogleOAuthLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GoogleOAuthLogic {
	return &GoogleOAuthLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GoogleOAuthLogic) GoogleOAuth(req *types.GoogleOAuthReq) (any, error) {
	resp, err := l.svcCtx.HcRpc.GoogleOAuth(l.ctx, &hcpb.GoogleOAuthRequest{Code: req.Code})
	if err != nil {
		return nil, err
	}
	return toData(resp), nil
}

// RefreshTokenLogic 刷新 Token
type RefreshTokenLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewRefreshTokenLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RefreshTokenLogic {
	return &RefreshTokenLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *RefreshTokenLogic) RefreshToken(req *types.RefreshTokenReq) (any, error) {
	resp, err := l.svcCtx.HcRpc.RefreshToken(l.ctx, &hcpb.RefreshTokenRequest{
		RefreshToken: req.RefreshToken,
	})
	if err != nil {
		return nil, err
	}
	return toData(resp), nil
}

// ChangePasswordLogic 修改密码（user_id 来自网关鉴权）
type ChangePasswordLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewChangePasswordLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ChangePasswordLogic {
	return &ChangePasswordLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ChangePasswordLogic) ChangePassword(req *types.ChangePasswordReq) (any, error) {
	uid, ok := requireUser(l.ctx)
	if !ok {
		return nil, errUserUnauthorized
	}
	_, err := l.svcCtx.HcRpc.ChangePassword(l.ctx, &hcpb.ChangePasswordRequest{
		UserId:      uid,
		OldPassword: req.OldPassword,
		NewPassword: req.NewPassword,
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{"message": "password changed"}, nil
}
