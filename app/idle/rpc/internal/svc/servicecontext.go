package svc

import (
	"github.com/cdcdx/hc-framework-go/app/idle/rpc/internal/config"
	userclient "github.com/cdcdx/hc-framework-go/app/user/rpc/user"
	"github.com/cdcdx/hc-framework-go/common/gormx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/zrpc"
	"gorm.io/gorm"
)

// ServiceContext idle-rpc 服务上下文
type ServiceContext struct {
	Config config.Config
	Db     *gorm.DB
	// UserRpc 积分入账客户端
	UserRpc userclient.User
}

// NewServiceContext 装配 DB 与 user-rpc 客户端
func NewServiceContext(c config.Config) *ServiceContext {
	db, err := gormx.Open(c.DB.Driver, c.DB.Dsn)
	if err != nil {
		logx.Must(err)
	}
	if err := db.AutoMigrate(&model.IdleRecord{}, &model.IdleDailyPoints{}); err != nil {
		logx.Must(err)
	}

	return &ServiceContext{
		Config:  c,
		Db:      db,
		UserRpc: userclient.NewUser(zrpc.MustNewClient(c.UserRpc).Conn()),
	}
}
