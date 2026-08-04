package config

import (
	gatewayconfig "github.com/cdcdx/hc-framework-go/app/gateway/api/config"
	rpcconfig "github.com/cdcdx/hc-framework-go/app/rpc/config"
)

// Config 合并部署配置：单进程同时承载 Gateway(REST) 与 Rpc(gRPC)。
// Gateway 与 Rpc 各自内嵌 ServiceConf，故用独立顶层 key 区分，避免字段冲突。
type Config struct {
	// Gateway REST 网关配置（rest.RestConf + Auth/Captcha/Security）
	Gateway gatewayconfig.Config

	// Rpc gRPC 服务配置（zrpc.RpcServerConf + DB/Jwt/BcryptCost/...）
	Rpc rpcconfig.Config
}
