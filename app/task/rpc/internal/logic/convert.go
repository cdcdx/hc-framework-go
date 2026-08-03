package logic

import (
	"fmt"
	"time"

	"github.com/cdcdx/hc-framework-go/app/task/rpc/task"
	"github.com/cdcdx/hc-framework-go/common/model"
)

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

// toTaskInfo 任务 + 进度 → rpc 返回结构；进度周期不匹配当前周期时按新周期展示（进度归零）
func toTaskInfo(t *model.Task, p *model.UserTaskProgress, currentPeriod string) *task.TaskInfo {
	info := &task.TaskInfo{
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
