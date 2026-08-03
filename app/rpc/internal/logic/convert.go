package logic

import (
	"errors"
	"fmt"
	"time"

	"github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/common/model"
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
