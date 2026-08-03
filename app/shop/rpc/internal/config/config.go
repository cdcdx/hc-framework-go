package config

import "github.com/zeromicro/go-zero/zrpc"

// Config shop-rpc 配置
type Config struct {
	zrpc.RpcServerConf

	DB struct {
		Driver string // sqlite / mysql / postgres
		Dsn    string
	}

	// UserRpc 扣积分；TaskRpc 上报兑换进度
	UserRpc zrpc.RpcClientConf
	TaskRpc zrpc.RpcClientConf
}
