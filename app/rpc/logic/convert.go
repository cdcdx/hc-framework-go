package logic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/app/rpc/svc"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

// isRecordNotFound 判断 gorm 记录不存在
func isRecordNotFound(err error) bool {
	return errors.Is(err, gorm.ErrRecordNotFound)
}

// ---------- user 域 ----------

// toUserInfo 用户模型 → rpc 结构
func toUserInfo(u *model.User) *hc.UserInfo {
	return &hc.UserInfo{
		UserId:        u.UserID,
		Username:      u.Username,
		Email:         u.Email,
		AvatarUrl:     u.AvatarURL,
		PointsBalance: u.PointsBalance,
		Status:        u.Status,
		CreatedAt:     u.CreatedAt.Unix(),
	}
}

// ---------- idle 域 ----------

// toIdleRecord 挂机记录模型 → rpc 结构
func toIdleRecord(r *model.IdleRecord) *hc.IdleRecord {
	var end int64
	if r.EndTime != nil {
		end = r.EndTime.Unix()
	}
	var hb int64
	if r.LastHeartbeatAt != nil {
		hb = r.LastHeartbeatAt.Unix()
	}
	return &hc.IdleRecord{
		Id:              r.ID,
		UserId:          r.UserID,
		DeviceId:        r.DeviceID,
		StartTime:       r.StartTime.Unix(),
		EndTime:         end,
		DurationSeconds: int32(r.DurationSeconds),
		PointsEarned:    r.PointsEarned,
		Status:          r.Status,
		LastHeartbeatAt: hb,
	}
}

// ---------- task 域 ----------

// periodOf 计算任务当前周期：
//   - daily → 当天 "2006-01-02"
//   - weekly → ISO 周 "2006-W01"
//   - achievement → "all"（永久累计）
func periodOf(t *model.Task, now time.Time) string {
	switch t.TaskType {
	case "daily":
		return now.Format("2006-01-02")
	case "weekly":
		y, w := now.ISOWeek()
		return fmt.Sprintf("%d-W%02d", y, w)
	default:
		return "all"
	}
}

// toTaskInfo 任务 + 进度 → rpc 结构；进度周期不匹配当前周期时按新周期展示（进度归零）
func toTaskInfo(t *model.Task, p *model.UserTaskProgress, currentPeriod string) *hc.TaskInfo {
	info := &hc.TaskInfo{
		Id:           t.ID,
		TaskType:     t.TaskType,
		TaskKey:      t.TaskKey,
		TaskName:     t.TaskName,
		TargetValue:  int32(t.TargetValue),
		RewardPoints: t.RewardPoints,
		IsActive:     t.IsActive,
		Period:       currentPeriod,
	}
	if p != nil && p.Period == currentPeriod {
		info.CurrentProgress = int32(p.CurrentProgress)
		info.IsCompleted = p.IsCompleted
		info.IsClaimed = p.IsClaimed
	}
	return info
}

// ---------- shop 域 ----------

// toItemInfo 商品模型 → rpc 结构（派生销售状态）
func toItemInfo(i *model.ShopItem) *hc.ItemInfo {
	i.SalesStatus = i.SalesStatusValue()
	return &hc.ItemInfo{
		Id:          i.ID,
		Name:        i.Name,
		Description: i.Description,
		PricePoints: i.PricePoints,
		Stock:       int32(i.Stock),
		ImageUrl:    i.ImageURL,
		Category:    i.Category,
		IsActive:    i.IsActive,
		SalesStatus: i.SalesStatus,
	}
}

// toOrderInfo 订单模型 → rpc 结构
func toOrderInfo(o *model.RedeemOrder) *hc.OrderInfo {
	return &hc.OrderInfo{
		Id:          o.ID,
		UserId:      o.UserID,
		ItemId:      o.ItemID,
		ItemName:    o.ItemName,
		PointsSpent: o.PointsSpent,
		OrderStatus: o.OrderStatus,
		ActivityId:  o.ActivityID,
		CreatedAt:   o.CreatedAt.Unix(),
	}
}

// toFlashActivity 抢购活动模型 → rpc 结构（派生销售状态）
func toFlashActivity(a *model.ShopFlashActivity) *hc.FlashActivityInfo {
	a.SalesStatus = a.SalesStatusValue()
	info := &hc.FlashActivityInfo{
		Id:           a.ID,
		ItemId:       a.ItemID,
		Name:         a.Name,
		StartTime:    a.StartTime.Unix(),
		LimitQty:     int32(a.LimitQty),
		SoldQty:      int32(a.SoldQty),
		PerUserLimit: int32(a.PerUserLimit),
		PricePoints:  a.PricePoints,
		Status:       a.Status,
		SalesStatus:  a.SalesStatus,
	}
	if a.EndTime != nil {
		info.EndTime = a.EndTime.Unix()
	}
	return info
}

// reportRedeemProgress 上报兑换相关任务进度。
// 异步化：向 MQ 投递 task_progress 事件，由后台消费者（ServiceContext.handleTaskProgress）
// 解耦处理，避免兑换主链路同步执行 3×(查Task+查/写Progress) 的 DB 开销。
// MQ 禁用时（noopProducer）退化为同步调用，行为与改造前一致。
func reportRedeemProgress(ctx context.Context, svcCtx *svc.ServiceContext, userID string, logger logx.Logger) {
	for _, key := range []string{
		model.TaskKeyDailyRedeem1,
		model.TaskKeyWeeklyRedeem3,
		model.TaskKeyAchieveRedeem100,
	} {
		// 同步路径（MQ 未启用）：保留原行为，保证功能可用。
		if svcCtx.MQProducer == nil {
			if _, err := NewReportProgressLogic(ctx, svcCtx).ReportProgress(&hc.ReportProgressRequest{
				UserId:  userID,
				TaskKey: key,
				Delta:   1,
			}); err != nil {
				logger.Errorf("report task progress %s failed: %v", key, err)
			}
			continue
		}
		// 异步路径：投递事件，fire-and-forget（MQ 内部已做缓冲/丢弃策略）。
		payload, err := json.Marshal(svc.TaskProgressEvent{UserID: userID, TaskKey: key, Delta: 1})
		if err != nil {
			logger.Errorf("marshal task progress %s failed: %v", key, err)
			continue
		}
		if err := svcCtx.Publish(ctx, "task_progress", userID, payload); err != nil {
			logger.Errorf("publish task progress %s failed: %v", key, err)
		}
	}
}

// loadTaskInfos 加载活跃任务列表并关联用户进度，转换为 rpc TaskInfo 列表。
// listlogic 和 progresslogic 共用，消除重复查询与组装逻辑。
// 任务定义（tasks）极少变动，使用缓存（key tasks:active，TTL 60s）避免每次全表扫描；
// 用户进度（user_task_progress）实时查询（已合并为单条 WHERE user_id=?）。
func loadTaskInfos(svcCtx *svc.ServiceContext, userID string) ([]*hc.TaskInfo, error) {
	var tasks []model.Task
	cacheKey := "tasks:active"
	cached, err := svcCtx.CachedGet(context.Background(), cacheKey, func(ctx context.Context) ([]byte, error) {
		var ts []model.Task
		if err := svcCtx.Db.Where("is_active = ?", true).Order("id ASC").Find(&ts).Error; err != nil {
			return nil, err
		}
		return json.Marshal(ts)
	}, 60*time.Second)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(cached, &tasks); err != nil {
		return nil, err
	}

	var pros []model.UserTaskProgress
	if err := svcCtx.Db.Where("user_id = ?", userID).Find(&pros).Error; err != nil {
		return nil, err
	}
	proMap := make(map[int64]*model.UserTaskProgress, len(pros))
	for i := range pros {
		proMap[pros[i].TaskID] = &pros[i]
	}

	now := time.Now()
	out := make([]*hc.TaskInfo, 0, len(tasks))
	for i := range tasks {
		out = append(out, toTaskInfo(&tasks[i], proMap[tasks[i].ID], periodOf(&tasks[i], now)))
	}
	return out, nil
}
