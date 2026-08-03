package svc

import (
	"github.com/cdcdx/hc-framework-go/app/task/rpc/internal/config"
	userclient "github.com/cdcdx/hc-framework-go/app/user/rpc/user"
	"github.com/cdcdx/hc-framework-go/common/gormx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/zrpc"
	"gorm.io/gorm"
)

// ServiceContext task-rpc 服务上下文
type ServiceContext struct {
	Config config.Config
	Db     *gorm.DB
	// UserRpc 奖励积分入账客户端
	UserRpc userclient.User
}

// NewServiceContext 装配 DB（含任务种子数据）与 user-rpc 客户端
func NewServiceContext(c config.Config) *ServiceContext {
	db, err := gormx.Open(c.DB.Driver, c.DB.Dsn)
	if err != nil {
		logx.Must(err)
	}
	if err := db.AutoMigrate(&model.Task{}, &model.UserTaskProgress{}); err != nil {
		logx.Must(err)
	}
	seedTasks(db)

	return &ServiceContext{
		Config:  c,
		Db:      db,
		UserRpc: userclient.NewUser(zrpc.MustNewClient(c.UserRpc)),
	}
}

// seedTasks 确保任务定义存在（幂等）
func seedTasks(db *gorm.DB) {
	defs := []model.Task{
		{TaskType: "daily", TaskKey: model.TaskKeyDailyLogin, TaskName: "每日登录", TargetValue: 1, RewardPoints: 10, IsActive: true},
		{TaskType: "daily", TaskKey: model.TaskKeyDailyIdle30, TaskName: "挂机 30 分钟", TargetValue: 30, RewardPoints: 20, IsActive: true},
		{TaskType: "daily", TaskKey: model.TaskKeyDailyRedeem1, TaskName: "完成 1 次兑换", TargetValue: 1, RewardPoints: 15, IsActive: true},
		{TaskType: "weekly", TaskKey: model.TaskKeyWeeklyIdle300, TaskName: "本周挂机 300 分钟", TargetValue: 300, RewardPoints: 100, IsActive: true},
		{TaskType: "weekly", TaskKey: model.TaskKeyWeeklyRedeem3, TaskName: "本周兑换 3 次", TargetValue: 3, RewardPoints: 60, IsActive: true},
		{TaskType: "achievement", TaskKey: model.TaskKeyAchievePoints10k, TaskName: "累计积分 10000", TargetValue: 10000, RewardPoints: 500, IsActive: true},
		{TaskType: "achievement", TaskKey: model.TaskKeyAchieveRedeem100, TaskName: "累计兑换 100 次", TargetValue: 100, RewardPoints: 2000, IsActive: true},
	}
	for _, t := range defs {
		var count int64
		if err := db.Model(&model.Task{}).Where("task_key = ?", t.TaskKey).Count(&count).Error; err != nil {
			logx.Must(err)
		}
		if count == 0 {
			if err := db.Create(&t).Error; err != nil {
				logx.Must(err)
			}
		}
	}
}
