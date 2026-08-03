package svc

import (
	"github.com/cdcdx/hc-framework-go/app/rpc/internal/config"
	"github.com/cdcdx/hc-framework-go/common/gormx"
	"github.com/cdcdx/hc-framework-go/common/jwt"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

// ServiceContext 单一 rpc 服务上下文：
// 合并 4 个域的 DB 表与 JWT 管理器；域间协作（积分/任务进度）为进程内调用，无需 rpc client。
type ServiceContext struct {
	Config config.Config
	Db     *gorm.DB
	JwtMgr *jwt.Manager
}

// NewServiceContext 装配 DB（含全部表结构与任务种子）与 JWT 管理器
func NewServiceContext(c config.Config) *ServiceContext {
	db, err := gormx.Open(c.DB.Driver, c.DB.Dsn)
	if err != nil {
		logx.Must(err)
	}
	if err := db.AutoMigrate(
		&model.User{},
		&model.IdleRecord{}, &model.IdleDailyPoints{},
		&model.Task{}, &model.UserTaskProgress{},
		&model.ShopItem{}, &model.RedeemOrder{}, &model.ShopFlashActivity{},
	); err != nil {
		logx.Must(err)
	}
	seedTasks(db)

	mgr, err := jwt.NewManager(
		c.Jwt.Algorithm, c.Jwt.SigningKey,
		c.Jwt.PrivateKeyPath, c.Jwt.PublicKeyPath,
		c.Jwt.Issuer, c.Jwt.AccessTTL, c.Jwt.RefreshTTL,
	)
	if err != nil {
		logx.Must(err)
	}

	return &ServiceContext{
		Config: c,
		Db:     db,
		JwtMgr: mgr,
	}
}
