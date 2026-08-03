package config

import (
	"time"

	"github.com/zeromicro/go-zero/zrpc"
)

// Config 单一领域 rpc 配置（合并 user/idle/task/shop 四个域）
type Config struct {
	zrpc.RpcServerConf

	DB struct {
		Driver string // sqlite / mysql / postgres
		Dsn    string
	}

	Jwt struct {
		Algorithm      string // HS256 / RS256
		SigningKey     string
		Issuer         string
		AccessTTL      time.Duration
		RefreshTTL     time.Duration
		PrivateKeyPath string
		PublicKeyPath  string
	}

	GoogleOAuth struct {
		ClientID     string
		ClientSecret string
		RedirectURI  string
	}

	Idle struct {
		MaxActiveDevices int
		PointsPerMinute  int
		DailyPointsLimit int64
	}
}
