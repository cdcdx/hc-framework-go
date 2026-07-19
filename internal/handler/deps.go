package handler

import (
	"github.com/cdcdx/hc-framework-go/internal/cache"
	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/db"
	"github.com/cdcdx/hc-framework-go/internal/repository"
	"github.com/cdcdx/hc-framework-go/internal/service/auth"
	"github.com/cdcdx/hc-framework-go/internal/service/idle"
	"github.com/cdcdx/hc-framework-go/internal/service/task"
	"github.com/cdcdx/hc-framework-go/internal/service/shop"
	"github.com/cdcdx/hc-framework-go/internal/service/common"
)

// Dependencies 依赖注入容器
type Dependencies struct {
	Cfg         *config.Config            // 初始配置快照（非动态组件使用）
	CfgMgr      *config.Manager           // 配置管理器（限流/熔断/安全/缓存降级等动态组件实时读取）
	BusinessDB  *db.RWDB                  // 业务数据库（支持读写分离）
	UserRepo    repository.UserRepository // 用户仓库（GORM 或 MongoDB）
	MonitorRepo repository.MonitorRepo    // 监控仓库（GORM / ClickHouse）
	LogRepo     repository.LogRepo        // 日志仓库（GORM / Elasticsearch）
	AuthSvc     *auth.AuthService
	IdleSvc     *idle.IdleService
	TaskSvc     *task.TaskService
	ShopSvc     *shop.ShopService
	LogSvc      *common.LogService
	CacheMgr    *cache.Manager
}
