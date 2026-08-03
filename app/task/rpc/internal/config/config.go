package config

import "github.com/zeromicro/go-zero/zrpc"

// Config task-rpc 配置
type Config struct {
	zrpc.RpcServerConf

	DB struct {
		Driver string // sqlite / mysql / postgres
		Dsn    string
	}

	// UserRpc 依赖 user-rpc 做奖励积分入账
	UserRpc zrpc.RpcClientConf
}
