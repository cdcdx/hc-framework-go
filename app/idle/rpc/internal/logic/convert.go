package logic

import (
	"errors"

	"github.com/cdcdx/hc-framework-go/app/idle/rpc/idle"
	"github.com/cdcdx/hc-framework-go/common/model"
	"gorm.io/gorm"
)

// isRecordNotFound 判断 gorm 记录不存在
func isRecordNotFound(err error) bool {
	return errors.Is(err, gorm.ErrRecordNotFound)
}

// toIdleRecord 领域模型 → rpc 返回结构
func toIdleRecord(r *model.IdleRecord) *idle.IdleRecord {
	var end int64
	if r.EndTime != nil {
		end = r.EndTime.Unix()
	}
	var hb int64
	if r.LastHeartbeatAt != nil {
		hb = r.LastHeartbeatAt.Unix()
	}
	return &idle.IdleRecord{
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
