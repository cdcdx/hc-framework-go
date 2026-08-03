package svc

import (
	"github.com/cdcdx/hc-framework-go/app/user/rpc/internal/config"
	"github.com/cdcdx/hc-framework-go/common/gormx"
	"github.com/cdcdx/hc-framework-go/common/jwt"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

// ServiceContext user-rpc 服务上下文
type ServiceContext struct {
	Config config.Config
	Db     *gorm.DB
	JwtMgr *jwt.Manager
}

// NewServiceContext 装配 DB 与 JWT 管理器
func NewServiceContext(c config.Config) *ServiceContext {
	db, err := gormx.Open(c.DB.Driver, c.DB.Dsn)
	if err != nil {
		logx.Must(err)
	}
	// 核心闭环：启动自动建表（与 gin 版表结构一致）
	if err := db.AutoMigrate(&model.User{}); err != nil {
		logx.Must(err)
	}

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
