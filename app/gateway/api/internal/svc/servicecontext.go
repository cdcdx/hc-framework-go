package svc

import (
	"github.com/cdcdx/hc-framework-go/app/gateway/api/internal/config"
	hcclient "github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/common/captcha"
	"github.com/cdcdx/hc-framework-go/common/jwt"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/zrpc"
)

// ServiceContext 网关服务上下文：单一领域 rpc 客户端 + JWT 管理器 + 验证码 Provider
type ServiceContext struct {
	Config config.Config

	// HcRpc 单一领域 rpc（含 user/idle/task/shop 全部方法）
	HcRpc hcclient.Hc

	JwtMgr *jwt.Manager

	CaptchaProvider captcha.CaptchaProvider
}

// NewServiceContext 装配全部依赖
func NewServiceContext(c config.Config) *ServiceContext {
	mgr, err := jwt.NewManager(
		c.Auth.Jwt.Algorithm, c.Auth.Jwt.SigningKey,
		"", "",
		c.Auth.Jwt.Issuer, c.Auth.Jwt.AccessTTL, c.Auth.Jwt.RefreshTTL,
	)
	if err != nil {
		logx.Must(err)
	}

	return &ServiceContext{
		Config:          c,
		HcRpc:           hcclient.NewHc(zrpc.MustNewClient(c.HcRpc)),
		JwtMgr:          mgr,
		CaptchaProvider: newCaptchaProvider(c),
	}
}
