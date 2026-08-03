package main

import (
	"flag"
	"fmt"

	"github.com/cdcdx/hc-framework-go/app/task/rpc/internal/config"
	"github.com/cdcdx/hc-framework-go/app/task/rpc/internal/server"
	"github.com/cdcdx/hc-framework-go/app/task/rpc/internal/svc"
	"github.com/cdcdx/hc-framework-go/app/task/rpc/task"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
)

var configFile = flag.String("f", "etc/task.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)
	ctx := svc.NewServiceContext(c)

	s := zrpc.MustNewServer(c.RpcServerConf, func(grpcServer *grpc.Server) {
		task.RegisterTaskServer(grpcServer, server.NewTaskServer(ctx))
	})
	defer s.Stop()

	fmt.Printf("Starting rpc server at %s...\n", c.ListenOn)
	s.Start()
}
