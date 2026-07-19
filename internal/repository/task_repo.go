package repository

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/cdcdx/hc-framework-go/internal/cache"
	"github.com/cdcdx/hc-framework-go/internal/db"
	"github.com/cdcdx/hc-framework-go/internal/model"
)

// TaskRepository 任务仓库
type TaskRepository struct {
	rw       *db.RWDB
	cacheMgr *cache.Manager // 三级缓存（可选）
}

// NewTaskRepository 创建任务仓库（读写分离）
func NewTaskRepository(rw *db.RWDB) *TaskRepository {
	return &TaskRepository{rw: rw}
}

// NewTaskRepositoryWithCache 创建带缓存的任务仓库
func NewTaskRepositoryWithCache(rw *db.RWDB, cacheMgr *cache.Manager) *TaskRepository {
	return &TaskRepository{rw: rw, cacheMgr: cacheMgr}
}

// NewTaskRepositoryFromTx 从事务 gorm.DB 创建任务仓库（事务内使用，强制走主库，不使用缓存）
func NewTaskRepositoryFromTx(tx *gorm.DB) *TaskRepository {
	return &TaskRepository{rw: db.NewRWDBFromGORM(tx)}
}

// FindAll 获取所有活跃任务（读从库 → 三级缓存）
func (r *TaskRepository) FindAll(ctx context.Context) ([]model.Task, error) {
	const cacheKey = "task:all"
	const ttl = 60 * time.Second

	if r.cacheMgr != nil {
		val, err := r.cacheMgr.Get(ctx, cacheKey, ttl, func(ctx context.Context) (interface{}, error) {
			var tasks []model.Task
			dbErr := r.rw.Read(ctx).Where("is_active = ?", true).Find(&tasks).Error
			return tasks, dbErr
		})
		if err != nil {
			return nil, err
		}
		// 缓存命中时 L1/L2(JSON) 反序列化为 []interface{}，不能直接断言为 []model.Task
		// （否则命中即 panic）。统一经 DecodeCached 还原，兼容回源直返与命中中间类型。
		return cache.DecodeCached[[]model.Task](val)
	}

	var tasks []model.Task
	err := r.rw.Read(ctx).Where("is_active = ?", true).Find(&tasks).Error
	return tasks, err
}

// FindByID 根据 ID 获取任务（读从库 → 三级缓存）
func (r *TaskRepository) FindByID(ctx context.Context, id int64) (*model.Task, error) {
	cacheKey := fmt.Sprintf("task:%d", id)
	const ttl = 120 * time.Second

	if r.cacheMgr != nil {
		val, err := r.cacheMgr.Get(ctx, cacheKey, ttl, func(ctx context.Context) (interface{}, error) {
			var task model.Task
			dbErr := r.rw.Read(ctx).First(&task, id).Error
			if dbErr != nil {
				if dbErr == gorm.ErrRecordNotFound {
					return nil, nil // 会触发空值缓存
				}
				return nil, dbErr
			}
			return &task, nil
		})
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil
		}
		return cache.DecodeCached[*model.Task](val)
	}

	var task model.Task
	err := r.rw.Read(ctx).First(&task, id).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &task, nil
}

// FindProgress 获取用户某个任务在指定周期的进度（读从库）
func (r *TaskRepository) FindProgress(ctx context.Context, userID string, taskID int64, period string) (*model.UserTaskProgress, error) {
	var progress model.UserTaskProgress
	err := r.rw.Read(ctx).
		Where("user_id = ? AND task_id = ? AND period = ?", userID, taskID, period).
		First(&progress).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &progress, nil
}

// FindUserProgress 获取用户所有任务进度（读从库）
func (r *TaskRepository) FindUserProgress(ctx context.Context, userID, period string) ([]model.UserTaskProgress, error) {
	var progresses []model.UserTaskProgress
	err := r.rw.Read(ctx).
		Where("user_id = ? AND period = ?", userID, period).
		Find(&progresses).Error
	return progresses, err
}

// UpsertProgress 创建或更新任务进度（原子 upsert，消除 TOCTOU 先读后写竞态）。
// 利用 uniqueIndex idx_user_task_period (user_id, task_id, period) 实现 ON CONFLICT 更新。
func (r *TaskRepository) UpsertProgress(ctx context.Context, progress *model.UserTaskProgress) error {
	return r.rw.Write(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "user_id"}, {Name: "task_id"}, {Name: "period"}},
		DoUpdates: clause.AssignmentColumns([]string{"current_progress", "is_completed", "is_claimed", "completed_at", "updated_at"}),
	}).Create(progress).Error
}

// FindByKeys 按 TaskKey 批量获取活跃任务（读从库）。事件驱动进度更新用。
func (r *TaskRepository) FindByKeys(ctx context.Context, keys []string) ([]model.Task, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	var tasks []model.Task
	err := r.rw.Read(ctx).
		Where("task_key IN ? AND is_active = ?", keys, true).
		Find(&tasks).Error
	return tasks, err
}

// isDeadlock 判断是否为 MySQL 死锁（errno 1213）或等价错误，用于 IncrProgress 重试。
// 高并发下 INSERT...ON DUPLICATE KEY UPDATE 因间隙锁顺序不同而死锁，属可重试的瞬时错误。
func isDeadlock(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Deadlock found") || strings.Contains(msg, "1213")
}

// IncrProgress 累加任务进度（事件驱动，写主库）。
//   - 使用原子 UPDATE SET current_progress = current_progress + delta 消除 TOCTOU 竞态。
//   - 达到 target 时标记完成并写 CompletedAt；已领取（IsClaimed）则不再累加。
//   - 幂等由上游事件去重 / 消费重试策略保证，本方法只做单调累加。
//   - 高并发下第一步 INSERT...ON DUPLICATE KEY UPDATE 可能死锁(1213)；整段加重试
//     （最多 3 次、指数退避），两步均幂等（单调累加 / DoNothing），重试安全。
func (r *TaskRepository) IncrProgress(ctx context.Context, userID string, task *model.Task, period string, delta int) error {
	if delta <= 0 {
		return nil
	}
	const maxRetry = 3
	var lastErr error
	for attempt := 0; attempt < maxRetry; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * 50 * time.Millisecond):
			}
		}
		lastErr = r.incrProgressOnce(ctx, userID, task, period, delta)
		if lastErr == nil || !isDeadlock(lastErr) {
			return lastErr
		}
		zap.L().Warn("IncrProgress deadlock, retrying",
			zap.String("user_id", userID), zap.Int64("task_id", task.ID),
			zap.Int("attempt", attempt+1), zap.Error(lastErr))
	}
	return lastErr
}

// incrProgressOnce 单次执行（见 IncrProgress 的重试封装）。两步均幂等，可被安全重试。
func (r *TaskRepository) incrProgressOnce(ctx context.Context, userID string, task *model.Task, period string, delta int) error {
	// 第一步：确保行存在（幂等插入，利用 uniqueIndex idx_user_task_period 去重）。
	// 注意：须捕获此处错误——死锁(1213)会在本步抛出，原实现忽略该错误会导致进度行未创建而丢失进度。
	if err := r.rw.Write(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "user_id"}, {Name: "task_id"}, {Name: "period"}},
		DoNothing: true,
	}).Create(&model.UserTaskProgress{
		UserID: userID, TaskID: task.ID, Period: period,
	}).Error; err != nil {
		return err
	}

	// 第二步：原子累加（WHERE is_claimed=false 保证已领取的不再累加）
	updates := map[string]interface{}{
		"current_progress": gorm.Expr("current_progress + ?", delta),
		"updated_at":       time.Now(),
	}
	if task.TargetValue > 0 {
		updates["is_completed"] = gorm.Expr("CASE WHEN current_progress + ? >= ? THEN true ELSE is_completed END", delta, task.TargetValue)
		updates["completed_at"] = gorm.Expr("CASE WHEN current_progress + ? >= ? AND is_completed = false THEN ? ELSE completed_at END", delta, task.TargetValue, time.Now())
	}

	result := r.rw.Write(ctx).Model(&model.UserTaskProgress{}).
		Where("user_id = ? AND task_id = ? AND period = ? AND is_claimed = ?", userID, task.ID, period, false).
		Updates(updates)
	return result.Error
}

// ClaimReward 领取任务奖励（写主库）
func (r *TaskRepository) ClaimReward(ctx context.Context, userID string, taskID int64, period string) error {
	result := r.rw.Write(ctx).Model(&model.UserTaskProgress{}).
		Where("user_id = ? AND task_id = ? AND period = ? AND is_completed = ? AND is_claimed = ?",
			userID, taskID, period, true, false).
		Update("is_claimed", true)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("task not completed or already claimed")
	}
	return nil
}

// SeedTasks 初始化默认任务数据（写主库）
func (r *TaskRepository) SeedTasks() error {
	var count int64
	r.rw.Master().Model(&model.Task{}).Count(&count)
	if count > 0 {
		return nil
	}

	tasks := []model.Task{
		{TaskType: "daily", TaskKey: model.TaskKeyDailyLogin, TaskName: "每日登录", TargetValue: 1, RewardPoints: 100, IsActive: true},
		{TaskType: "daily", TaskKey: model.TaskKeyDailyIdle30, TaskName: "挂机满30分钟", TargetValue: 30, RewardPoints: 200, IsActive: true},
		{TaskType: "daily", TaskKey: model.TaskKeyDailyRedeem1, TaskName: "完成1次兑换", TargetValue: 1, RewardPoints: 300, IsActive: true},
		{TaskType: "weekly", TaskKey: model.TaskKeyWeeklyIdle300, TaskName: "累计挂机300分钟", TargetValue: 300, RewardPoints: 1000, IsActive: true},
		{TaskType: "weekly", TaskKey: model.TaskKeyWeeklyRedeem3, TaskName: "累计兑换3次", TargetValue: 3, RewardPoints: 1500, IsActive: true},
		{TaskType: "achievement", TaskKey: model.TaskKeyAchievePoints10k, TaskName: "累计获得10000积分", TargetValue: 10000, RewardPoints: 5000, IsActive: true},
		{TaskType: "achievement", TaskKey: model.TaskKeyAchieveRedeem100, TaskName: "兑换100次", TargetValue: 100, RewardPoints: 10000, IsActive: true},
	}

	if err := r.rw.Master().Create(&tasks).Error; err != nil {
		return err
	}

	// 写入后失效缓存
	r.invalidateCache(context.Background(), "task:all")
	for _, t := range tasks {
		r.invalidateCache(context.Background(), fmt.Sprintf("task:%d", t.ID))
	}
	return nil
}

// invalidateCache 失效缓存（内部辅助，不报错）
func (r *TaskRepository) invalidateCache(ctx context.Context, keys ...string) {
	if r.cacheMgr == nil {
		return
	}
	_ = r.cacheMgr.Delete(ctx, keys...)
}

// ResetProgressByType 重置指定类型任务的进度：删除非当前周期的用户进度行。
// 通过子查询关联 tasks 表按 task_type 过滤，避免误删其他类型进度。
// currentPeriod 应为 repository.GetCurrentPeriod(taskType) 的返回值。
func (r *TaskRepository) ResetProgressByType(ctx context.Context, taskType, currentPeriod string) (int64, error) {
	result := r.rw.Write(ctx).
		Where("task_id IN (SELECT id FROM tasks WHERE task_type = ?) AND period != ?", taskType, currentPeriod).
		Delete(&model.UserTaskProgress{})
	if result.Error != nil {
		return 0, result.Error
	}
	return result.RowsAffected, nil
}

// GetCurrentPeriod 获取当前任务周期标识
func GetCurrentPeriod(taskType string) string {
	now := time.Now().UTC()
	switch taskType {
	case "daily":
		return now.Format("2006-01-02")
	case "weekly":
		// 计算本周一的日期
		weekday := now.Weekday()
		if weekday == time.Sunday {
			weekday = 7
		}
		monday := now.AddDate(0, 0, -int(weekday-time.Monday))
		return monday.Format("2006-01-02")
	case "achievement":
		return "lifetime"
	default:
		return now.Format("2006-01-02")
	}
}

// AutoMigrate 自动迁移（操作主库 DDL）
func (r *TaskRepository) AutoMigrate() error {
	return r.rw.Master().AutoMigrate(&model.Task{}, &model.UserTaskProgress{}, &model.EventDedup{})
}
