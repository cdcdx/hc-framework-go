package config

import (
	"time"

	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

// Config 网关配置
type Config struct {
	rest.RestConf

	Auth struct {
		Jwt struct {
			Algorithm  string
			SigningKey string
			Issuer     string
			AccessTTL  time.Duration
			RefreshTTL time.Duration
		}
	}

	Captcha struct {
		Enabled   bool
		Provider  string // none / turnstile / recaptcha / hcaptcha / tencent
		SiteKey   string
		SecretKey string
		AppID     string
	}

	Security struct {
		IPLimitEnabled   bool
		IPLimitPerMinute int
	}

	UserRpc zrpc.RpcClientConf
	IdleRpc zrpc.RpcClientConf
	TaskRpc zrpc.RpcClientConf
	ShopRpc zrpc.RpcClientConf
}
