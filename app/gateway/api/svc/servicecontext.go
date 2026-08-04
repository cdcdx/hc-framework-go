package svc

import (
	"github.com/cdcdx/hc-framework-go/app/gateway/api/config"
	hcclient "github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/common/captcha"
	"github.com/cdcdx/hc-framework-go/common/jwt"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/zrpc"
)

// ServiceContext 网关服务上下文：单一领域 rpc 客户端 + JWT 管理器 + 验证码 Provider
type ServiceContext struct {
	Config config.Config

	// HcRpc 单一领域 rpc（含 user/idle/task/shop 全部方法）。
	// 合并部署时由外部注入进程内 LocalHcClient；独立部署时由 zrpc client 创建。
	HcRpc hcclient.Hc

	JwtMgr *jwt.Manager

	CaptchaProvider captcha.CaptchaProvider
}

func newCaptchaProvider(c config.Config) captcha.CaptchaProvider {
	if !c.Captcha.Enabled {
		return &captcha.NoopVerifier{}
	}
	switch c.Captcha.Provider {
	case "turnstile":
		return captcha.NewTurnstileVerifier(c.Captcha.SiteKey, c.Captcha.SecretKey)
	case "recaptcha":
		return captcha.NewReCAPTCHAVerifier(c.Captcha.SiteKey, c.Captcha.SecretKey)
	case "hcaptcha":
		return captcha.NewhCAPTCHAVerifier(c.Captcha.SiteKey, c.Captcha.SecretKey)
	case "tencent":
		return captcha.NewTencentVerifier(c.Captcha.AppID, c.Captcha.SecretKey)
	default:
		return &captcha.NoopVerifier{}
	}
}

// NewServiceContext 独立部署时装配全部依赖（创建 gRPC 客户端连接 rpc）。
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

// NewServiceContextWithClient 合并部署时装配依赖（HcRpc 由外部注入进程内客户端）。
func NewServiceContextWithClient(c config.Config, hcClient hcclient.Hc) *ServiceContext {
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
		HcRpc:           hcClient,
		JwtMgr:          mgr,
		CaptchaProvider: newCaptchaProvider(c),
	}
}
