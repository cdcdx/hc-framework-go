package svc

import (
	"github.com/cdcdx/hc-framework-go/app/shop/rpc/internal/config"
	taskclient "github.com/cdcdx/hc-framework-go/app/task/rpc/task"
	userclient "github.com/cdcdx/hc-framework-go/app/user/rpc/user"
	"github.com/cdcdx/hc-framework-go/common/gormx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/zrpc"
	"gorm.io/gorm"
)

// ServiceContext shop-rpc 服务上下文
type ServiceContext struct {
	Config config.Config
	Db     *gorm.DB
	// UserRpc 积分扣减；TaskRpc 任务进度上报
	UserRpc userclient.User
	TaskRpc taskclient.Task
}

// NewServiceContext 装配 DB 与依赖 rpc 客户端
func NewServiceContext(c config.Config) *ServiceContext {
	db, err := gormx.Open(c.DB.Driver, c.DB.Dsn)
	if err != nil {
		logx.Must(err)
	}
	if err := db.AutoMigrate(
		&model.ShopItem{},
		&model.RedeemOrder{},
		&model.ShopFlashActivity{},
	); err != nil {
		logx.Must(err)
	}

	return &ServiceContext{
		Config:  c,
		Db:      db,
		UserRpc: userclient.NewUser(zrpc.MustNewClient(c.UserRpc)),
		TaskRpc: taskclient.NewTask(zrpc.MustNewClient(c.TaskRpc)),
	}
}
