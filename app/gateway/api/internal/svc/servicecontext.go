package svc

import (
	"github.com/cdcdx/hc-framework-go/app/gateway/api/internal/config"
	idleclient "github.com/cdcdx/hc-framework-go/app/idle/rpc/idle"
	shopclient "github.com/cdcdx/hc-framework-go/app/shop/rpc/shop"
	taskclient "github.com/cdcdx/hc-framework-go/app/task/rpc/task"
	userclient "github.com/cdcdx/hc-framework-go/app/user/rpc/user"
	"github.com/cdcdx/hc-framework-go/common/captcha"
	"github.com/cdcdx/hc-framework-go/common/jwt"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/zrpc"
)

// ServiceContext 网关服务上下文：4 个领域 rpc 客户端 + JWT 管理器 + 验证码 Provider
type ServiceContext struct {
	Config config.Config

	UserRpc userclient.User
	IdleRpc idleclient.Idle
	TaskRpc taskclient.Task
	ShopRpc shopclient.Shop

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
		UserRpc:         userclient.NewUser(zrpc.MustNewClient(c.UserRpc)),
		IdleRpc:         idleclient.NewIdle(zrpc.MustNewClient(c.IdleRpc)),
		TaskRpc:         taskclient.NewTask(zrpc.MustNewClient(c.TaskRpc)),
		ShopRpc:         shopclient.NewShop(zrpc.MustNewClient(c.ShopRpc)),
		JwtMgr:          mgr,
		CaptchaProvider: newCaptchaProvider(c),
	}
}

// newCaptchaProvider 按配置构建验证码 Provider（未启用返回 nil）
func newCaptchaProvider(c config.Config) captcha.CaptchaProvider {
	if !c.Captcha.Enabled {
		return nil
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
		logx.Must(errUnknownCaptchaProvider(c.Captcha.Provider))
		return nil
	}
}
