package main

import (
	"flag"
	"fmt"

	gatewayhandler "github.com/cdcdx/hc-framework-go/app/gateway/api/handler"
	gatewaysvc "github.com/cdcdx/hc-framework-go/app/gateway/api/svc"
	"github.com/cdcdx/hc-framework-go/app/rpc/server"
	rpcsvc "github.com/cdcdx/hc-framework-go/app/rpc/svc"
	"github.com/cdcdx/hc-framework-go/app/server/internal/config"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/prometheus"
	"github.com/zeromicro/go-zero/rest"
)

var configFile = flag.String("f", "config/server.yaml", "the config file")

// main 合并部署入口：单进程承载 Gateway(REST)，通过进程内 LocalHcClient 直接调用
// Rpc logic（同一进程内函数调用，省去跨进程 gRPC 与服务发现）。
// 旧的 gRPC 对外接口已移除，仅保留 REST。
func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)

	// ---- Rpc 后端（同进程）----
	rpcCtx := rpcsvc.NewServiceContext(c.Rpc)
	defer rpcCtx.Close()
	hcServer := server.NewHcServer(rpcCtx)
	// 进程内客户端，供 Gateway 直接调用，无需 gRPC 网络连接。
	localHc := server.NewLocalHcClient(hcServer)

	// ---- Gateway(REST) 侧 ----
	gwCtx := gatewaysvc.NewServiceContextWithClient(c.Gateway, localHc)
	// 启用 go-zero 原生指标（http_server_requests_* 等）。
	// go-zero 的 metric 包在 prometheus.Enabled() 为 false 时会直接跳过注册，
	// 因此必须显式 Enable。指标注册进默认 registry，
	// 由网关 /metrics（promhttp.Handler()）统一暴露，无需另起端口。
	prometheus.Enable()
	gwServer := rest.MustNewServer(c.Gateway.RestConf)
	defer gwServer.Stop()
	gatewayhandler.RegisterHandlers(gwServer, gwCtx)

	// Prometheus 指标由网关 /metrics（见 handler/routes.go 的 promhttp.Handler()）统一暴露，
	// 不再单独监听端口。合并后 gateway 与 rpc 共享同一 prometheus 注册表，单端点即可。

	fmt.Printf("Starting combined server (REST :%d, metrics :%d/metrics)...\n",
		c.Gateway.Port, c.Gateway.Port)

	gwServer.Start()
}
