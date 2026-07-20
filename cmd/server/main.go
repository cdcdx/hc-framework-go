package main

// @title           High Concurrency Framework API
// @version         1.0.0
// @description     高并发后台框架 API 文档
// @host            localhost:8080
// @BasePath        /
// @securityDefinitions.apikey BearerAuth
// @in header
// @name Authorization

import (
	"flag"
	"fmt"
	"os"

	"github.com/cdcdx/hc-framework-go/internal/bootstrap"
	"github.com/cdcdx/hc-framework-go/internal/metrics"
	_ "github.com/cdcdx/hc-framework-go/swagger"
)

// version / commit 不在此写死，完全由构建系统（Makefile）通过 -ldflags -X main.version /
// -X main.commit 注入；git 工作区有未提交修改时 commit 自动带 -dirty 后缀。
// 未注入（如直接 go run）时两者均为空字符串，hc_build_info 仅暴露 goversion。
var (
	version string
	commit  string
)

// main 仅作薄入口：解析 -config / -v 后把整个生命周期交给 bootstrap.Run
// （配置加载 → DB/服务/路由装配 → 后台任务与 HTTP 服务 → 信号阻塞 → 优雅关闭）。
// 原上帝文件（944 行）的编排逻辑已下沉到 internal/bootstrap（App.Run）。
// 所有命令行参数（含 -config）统一在此处用唯一的 flag.CommandLine 解析一次，
// 避免与 bootstrap.Run 内的 flag.Parse 重复/顺序冲突导致 "flag provided but not defined"。
func main() {
	// -config：配置文件路径，默认 config/config.yaml（run-stress 会显式覆盖）。
	configPath := flag.String("config", "config/config.yaml", "配置文件路径")
	// -v / -version：仅打印 版本号 + commit 后退出，用于快速确认构建产物来源。
	showVersion := flag.Bool("v", false, "打印版本号与 commit 后退出")
	flag.BoolVar(showVersion, "version", false, "打印版本号与 commit 后退出")
	flag.Parse()
	if *showVersion {
		fmt.Printf("version: %s-%s\n", version, commit)
		os.Exit(0)
	}

	// 把构建版本信息暴露到 hc_build_info，便于监控按版本分组、检测发版。
	metrics.SetBuildInfo(version, commit)
	os.Exit(bootstrap.Run(*configPath))
}
