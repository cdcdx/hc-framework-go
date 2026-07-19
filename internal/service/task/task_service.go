package task

import (
	"github.com/cdcdx/hc-framework-go/internal/service/common"
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/cdcdx/hc-framework-go/internal/cache"
	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/db"
	"github.com/cdcdx/hc-framework-go/internal/event"
	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/cdcdx/hc-framework-go/internal/repository"
)

// TaskService 任务服务
type TaskService struct {
	cfg        *config.Config
	taskRepo   *repository.TaskRepository
	userRepo   repository.UserRepository
	shopRepo   *repository.ShopRepository
	businessDB *db.RWDB
	logSvc     *common.LogService
	points     *common.PointsOutboxApplier // 积分可靠投递（Outbox），保证跨库最终一致
}

// NewTaskService 创建任务服务（userRepo 存积分，businessDB 存任务数据）
func NewTaskService(cfg *config.Config, userRepo repository.UserRepository, businessDB *db.RWDB, logSvc *common.LogService, cacheMgr *cache.Manager, points *common.PointsOutboxApplier) *TaskService {
	return &TaskService{
		cfg:        cfg,
		taskRepo:   repository.NewTaskRepositoryWithCache(businessDB, cacheMgr),
		userRepo:   userRepo,
		shopRepo:   repository.NewShopRepositoryWithCache(businessDB, cacheMgr),
		businessDB: businessDB,
		logSvc:     logSvc,
		points:     points,
	}
}

// TaskWithProgress 任务及用户进度
type TaskWithProgress struct {
	Task     model.Task              `json:"task"`
	Progress *model.UserTaskProgress `json:"progress,omitempty"`
}

// List 获取任务列表（含用户进度）。
// 优化：按 period 分组，每 period 仅一次批量查询进度，消除 N+1 问题。
func (s *TaskService) List(ctx context.Context, userID string) ([]TaskWithProgress, error) {
	tasks, err := s.taskRepo.FindAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("find tasks: %w", err)
	}

	// 按 period 分组，每 period 一次批量查询进度
	progressByPeriod := make(map[string][]model.UserTaskProgress)
	periods := make(map[string]bool)
	for _, task := range tasks {
		periods[repository.GetCurrentPeriod(task.TaskType)] = true
	}
	for period := range periods {
		plist, err := s.taskRepo.FindUserProgress(ctx, userID, period)
		if err != nil {
			return nil, fmt.Errorf("find progress for period %s: %w", period, err)
		}
		progressByPeriod[period] = plist
	}

	// 按 taskID 索引进度
	progressMap := make(map[int64]*model.UserTaskProgress)
	for period := range progressByPeriod {
		for i := range progressByPeriod[period] {
			p := &progressByPeriod[period][i]
			progressMap[p.TaskID] = p
		}
	}

	result := make([]TaskWithProgress, 0, len(tasks))
	for _, task := range tasks {
		twp := TaskWithProgress{Task: task}
		if p, ok := progressMap[task.ID]; ok {
			twp.Progress = p
		}
		result = append(result, twp)
	}

	return result, nil
}

// Progress 查询任务进度详情。
// 优化：按 period 分组，每 period 仅一次批量查询进度，消除 N+1 问题。
func (s *TaskService) Progress(ctx context.Context, userID string) (map[string]any, error) {
	tasks, err := s.taskRepo.FindAll(ctx)
	if err != nil {
		return nil, err
	}

	// 按 period 分组，每 period 一次批量查询进度
	progressByPeriod := make(map[string][]model.UserTaskProgress)
	periods := make(map[string]bool)
	for _, task := range tasks {
		periods[repository.GetCurrentPeriod(task.TaskType)] = true
	}
	for period := range periods {
		plist, err := s.taskRepo.FindUserProgress(ctx, userID, period)
		if err != nil {
			return nil, fmt.Errorf("find progress for period %s: %w", period, err)
		}
		progressByPeriod[period] = plist
	}

	// 按 taskID 索引进度
	progressMap := make(map[int64]*model.UserTaskProgress)
	for period := range progressByPeriod {
		for i := range progressByPeriod[period] {
			p := &progressByPeriod[period][i]
			progressMap[p.TaskID] = p
		}
	}

	type ProgressInfo struct {
		TaskID       int64  `json:"task_id"`
		TaskName     string `json:"task_name"`
		TaskType     string `json:"task_type"`
		TaskKey      string `json:"task_key"`
		TargetValue  int    `json:"target_value"`
		RewardPoints int64  `json:"reward_points"`
		Current      int    `json:"current"`
		IsCompleted  bool   `json:"is_completed"`
		IsClaimed    bool   `json:"is_claimed"`
	}

	result := make([]ProgressInfo, 0, len(tasks))
	for _, task := range tasks {
		pi := ProgressInfo{
			TaskID:       task.ID,
			TaskName:     task.TaskName,
			TaskType:     task.TaskType,
			TaskKey:      task.TaskKey,
			TargetValue:  task.TargetValue,
			RewardPoints: task.RewardPoints,
		}
		if p, ok := progressMap[task.ID]; ok {
			pi.Current = p.CurrentProgress
			pi.IsCompleted = p.IsCompleted
			pi.IsClaimed = p.IsClaimed
		}
		result = append(result, pi)
	}

	return map[string]any{
		"tasks": result,
	}, nil
}

// Claim 领取任务奖励
func (s *TaskService) Claim(ctx context.Context, userID string, taskID int64) (*model.Task, error) {
	task, err := s.taskRepo.FindByID(ctx, taskID)
	if err != nil {
		return nil, fmt.Errorf("find task: %w", err)
	}
	if task == nil {
		return nil, ErrTaskNotFound
	}

	period := repository.GetCurrentPeriod(task.TaskType)

	// 检查进度
	progress, err := s.taskRepo.FindProgress(ctx, userID, taskID, period)
	if err != nil {
		return nil, err
	}
	if progress == nil || !progress.IsCompleted {
		return nil, ErrTaskNotCompleted
	}
	if progress.IsClaimed {
		return nil, ErrTaskClaimed
	}

	// 在 businessDB 事务中标记领取 + 写积分流水 + 写积分 outbox（同提交）
	var outboxRec *model.PointsOutbox
	err = s.businessDB.Transaction(ctx, func(tx *gorm.DB) error {
		taskRepo := repository.NewTaskRepositoryFromTx(tx)
		shopRepo := repository.NewShopRepositoryFromTx(tx)

		// 标记已领取
		if err := taskRepo.ClaimReward(ctx, userID, taskID, period); err != nil {
			return err
		}

		// 写积分流水
		txRecord := &model.PointsTransaction{
			UserID:       userID,
			ChangeAmount: task.RewardPoints,
			ChangeType:   model.ChangeTypeTaskReward,
			ReferenceID:  fmt.Sprintf("task:%d", taskID),
		}
		if err := shopRepo.CreateTransaction(ctx, txRecord); err != nil {
			return fmt.Errorf("create points transaction: %w", err)
		}
		// 写积分 outbox（与以上同提交）：事务提交后由 Outbox 投递器可靠更新 userDB 余额。
		rec := &model.PointsOutbox{
			UserID:    userID,
			Delta:     task.RewardPoints,
			RefType:   "task",
			RefID:     fmt.Sprintf("%d", taskID),
			EventID:   fmt.Sprintf("pts:task:%s:%d:%s", userID, taskID, period),
			Status:    model.OutboxStatusPending,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		if s.points != nil {
			if err := s.points.AppendOutboxInTx(tx, rec); err != nil {
				return fmt.Errorf("append points outbox: %w", err)
			}
		}
		outboxRec = rec
		return nil
	})
	if err != nil {
		return nil, err
	}

	// 跨库一致：通过 Outbox 可靠投递积分调整（businessDB 已提交 outbox 记录），
	// 同步快路径 + Kafka 消费者 + 后台 relay 竞争应用；userDB 失败也不影响本次领取结果。
	if s.points != nil {
		s.points.ApplyAsync(ctx, outboxRec)
	}

	// 写任务完成日志 + 监控指标
	s.logSvc.LogTaskComplete(ctx, common.BuildMeta(ctx, userID), task.ID, task.TaskName, task.RewardPoints)

	return task, nil
}

// ApplyEventProgress 根据 Kafka 事件驱动更新任务进度（由 Consumer 调用）。
// 通过 event_id 幂等键 + event_dedup 表，在事务内「先标记去重、再累加进度」实现 effectively-once：
//   - idle.settled：按挂机分钟数累加挂机类任务（daily_idle_30 / weekly_idle_300）。
//   - shop.redeemed：兑换类任务各 +1（daily_redeem_1 / weekly_redeem_3 / achieve_redeem_100）。
//
// 未知事件类型静默忽略并返回 nil（避免消费端反复重试无关消息）。
// 重复消费（重试/再均衡重投）会因 event_dedup 唯一冲突被跳过，不会重复累加。
func (s *TaskService) ApplyEventProgress(ctx context.Context, msg *event.Message) error {
	if msg == nil {
		return nil
	}
	var eventID, userID string
	var keys []string
	var delta int
	switch msg.EventType {
	case event.EventIdleSettled:
		var p event.IdleSettledPayload
		if err := common.DecodePayload(msg.Payload, &p); err != nil {
			return err
		}
		// 优先用消息自带 event_id；缺失时（旧生产者/降级）用业务主键派生，保证幂等
		eventID = msg.EventID
		if eventID == "" {
			eventID = fmt.Sprintf("idle:%d", p.IdleRecordID)
		}
		userID, delta = p.UserID, p.DurationSeconds/60
		if delta <= 0 {
			return nil
		}
		// 注意：task_key 必须与 SeedTasks 的挂机任务一致（统一引用 model 常量，避免漂移）
		keys = []string{model.TaskKeyDailyIdle30, model.TaskKeyWeeklyIdle300}

	case event.EventShopRedeemed:
		var p event.ShopRedeemedPayload
		if err := common.DecodePayload(msg.Payload, &p); err != nil {
			return err
		}
		eventID = msg.EventID
		if eventID == "" {
			eventID = fmt.Sprintf("shop:%d", p.OrderID)
		}
		userID, delta = p.UserID, 1
		keys = []string{model.TaskKeyDailyRedeem1, model.TaskKeyWeeklyRedeem3, model.TaskKeyAchieveRedeem100}

	default:
		return nil
	}
	if userID == "" || delta <= 0 {
		return nil
	}

	dedup := repository.NewEventDedupRepository(s.businessDB)
	// 整段“去重标记 + 进度累加”在同一事务内：去重冲突则整体回滚，绝不重复累加
	return s.businessDB.Transaction(ctx, func(tx *gorm.DB) error {
		first, err := dedup.Mark(tx, eventID, msg.EventType)
		if err != nil {
			return fmt.Errorf("mark event dedup: %w", err)
		}
		if !first {
			return nil // 已处理过，幂等跳过
		}
		taskRepo := repository.NewTaskRepositoryFromTx(tx)
		// 事务内仓库（FromTx）强制走 tx（主库），无需再标记 UseMaster
		tasks, err := taskRepo.FindByKeys(ctx, keys)
		if err != nil {
			return fmt.Errorf("find tasks by keys: %w", err)
		}
		for i := range tasks {
			t := &tasks[i]
			period := repository.GetCurrentPeriod(t.TaskType)
			if err := taskRepo.IncrProgress(ctx, userID, t, period, delta); err != nil {
				return fmt.Errorf("incr progress task=%s: %w", t.TaskKey, err)
			}
		}
		return nil
	})
}



// SeedTasks 初始化任务数据
func (s *TaskService) SeedTasks() error {
	return s.taskRepo.SeedTasks()
}

// CleanupDedup 清理 retention 之前的事件去重记录（定时任务调用），返回清理行数。
// retention<=0 时使用默认 7 天。
func (s *TaskService) CleanupDedup(ctx context.Context, retention time.Duration) (int64, error) {
	if retention <= 0 {
		retention = 7 * 24 * time.Hour
	}
	dedup := repository.NewEventDedupRepository(s.businessDB)
	n, err := dedup.DeleteBefore(ctx, time.Now().Add(-retention))
	if err != nil {
		return 0, fmt.Errorf("cleanup event dedup: %w", err)
	}
	return n, nil
}

// ResetPeriod 重置指定类型任务的进度（每日/每周定时任务调用）。
// 删除该类型下非当前周期的用户进度行，防止数据无限增长，并使新周期进度从 0 开始。
// 返回被清理的进度行数量。成就类（achievement，period=lifetime）不应调用本方法。
func (s *TaskService) ResetPeriod(ctx context.Context, taskType string) (int64, error) {
	currentPeriod := repository.GetCurrentPeriod(taskType)
	cleared, err := s.taskRepo.ResetProgressByType(ctx, taskType, currentPeriod)
	if err != nil {
		return 0, fmt.Errorf("reset %s progress: %w", taskType, err)
	}
	return cleared, nil
}

var (
	ErrTaskNotFound     = fmt.Errorf("task not found")
	ErrTaskNotCompleted = fmt.Errorf("task not completed")
	ErrTaskClaimed      = fmt.Errorf("task already claimed")
)
