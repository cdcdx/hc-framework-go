package logic

import (
	"context"

	"github.com/cdcdx/hc-framework-go/app/gateway/api/svc"
	"github.com/zeromicro/go-zero/core/logx"
)

// HealthLogic 健康检查
type HealthLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewHealthLogic(ctx context.Context, svcCtx *svc.ServiceContext) *HealthLogic {
	return &HealthLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *HealthLogic) Health() (any, error) {
	return map[string]any{"status": "ok"}, nil
}

// ReadyLogic 就绪检查
type ReadyLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewReadyLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ReadyLogic {
	return &ReadyLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ReadyLogic) Ready() (any, error) {
	return map[string]any{"status": "ready"}, nil
}

// CaptchaConfigLogic 验证码前端配置
type CaptchaConfigLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCaptchaConfigLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CaptchaConfigLogic {
	return &CaptchaConfigLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *CaptchaConfigLogic) CaptchaConfig() (any, error) {
	cfg := l.svcCtx.Config.Captcha
	resp := map[string]any{
		"enabled":  cfg.Enabled,
		"provider": cfg.Provider,
		"site_key": cfg.SiteKey,
	}
	if p := l.svcCtx.CaptchaProvider; p != nil {
		resp["script_src"] = p.GetScriptSrc()
	}
	return resp, nil
}
