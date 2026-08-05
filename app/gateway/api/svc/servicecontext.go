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

	// zrpcClient 仅当独立部署（NewServiceContext）自建 zrpc 客户端时非 nil，
	// 用于进程退出时优雅关闭底层 gRPC 连接；合并部署注入的 client 不可关闭（ownRPC=false）。
	zrpcClient zrpc.Client
	ownRPC     bool

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
	cli := zrpc.MustNewClient(c.HcRpc)
	return &ServiceContext{
		Config:          c,
		HcRpc:           hcclient.NewHc(cli),
		zrpcClient:      cli,
		ownRPC:          true,
		JwtMgr:          newJwtManager(c),
		CaptchaProvider: newCaptchaProvider(c),
	}
}

// NewServiceContextWithClient 合并部署时装配依赖（HcRpc 由外部注入进程内客户端）。
func NewServiceContextWithClient(c config.Config, hcClient hcclient.Hc) *ServiceContext {
	return &ServiceContext{
		Config:          c,
		HcRpc:           hcClient,
		ownRPC:          false,
		JwtMgr:          newJwtManager(c),
		CaptchaProvider: newCaptchaProvider(c),
	}
}

// Close 优雅关闭网关持有的外部资源：仅当独立部署自建 zrpc client 时关闭底层 gRPC 连接。
// 合并部署（NewServiceContextWithClient）注入的 client 由外部进程生命周期管理，此处不关闭。
// 重复调用安全（zrpc.Client.Conn() 的 Close 幂等，重复关闭仅返回错误，已被忽略）。
func (svc *ServiceContext) Close() {
	if svc.ownRPC && svc.zrpcClient != nil {
		if err := svc.zrpcClient.Conn().Close(); err != nil {
			logx.Errorf("[gateway] close rpc conn failed: %v", err)
		}
	}
}

// newJwtManager 构建 JWT 管理器，集中处理配置到 jwt.NewManager 的参数映射与失败退出。
func newJwtManager(c config.Config) *jwt.Manager {
	mgr, err := jwt.NewManager(
		c.Auth.Jwt.Algorithm, c.Auth.Jwt.SigningKey,
		"", "",
		c.Auth.Jwt.Issuer, c.Auth.Jwt.AccessTTL, c.Auth.Jwt.RefreshTTL,
	)
	if err != nil {
		logx.Must(err)
	}
	return mgr
}
