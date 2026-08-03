package config

import "github.com/zeromicro/go-zero/zrpc"

// Config idle-rpc 配置
type Config struct {
	zrpc.RpcServerConf

	DB struct {
		Driver string // sqlite / mysql / postgres
		Dsn    string
	}

	Idle struct {
		MaxActiveDevices int
		PointsPerMinute  int
		DailyPointsLimit int64
	}

	// UserRpc 依赖 user-rpc 做积分入账
	UserRpc zrpc.RpcClientConf
}
