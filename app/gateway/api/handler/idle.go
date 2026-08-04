package handler

import (
	"net/http"

	"github.com/cdcdx/hc-framework-go/app/gateway/api/logic"
	"github.com/cdcdx/hc-framework-go/app/gateway/api/svc"
	"github.com/cdcdx/hc-framework-go/app/gateway/api/types"
	"github.com/cdcdx/hc-framework-go/common/response"
	"github.com/zeromicro/go-zero/rest/httpx"
)

// IdleStartHandler 开始挂机
func IdleStartHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.IdleStartReq
		if err := httpx.Parse(r, &req); err != nil {
			response.BadRequest(w, r, "device_id is required")
			return
		}
		l := logic.NewIdleStartLogic(r.Context(), svcCtx)
		resp, err := l.IdleStart(&req)
		if err != nil {
			handleError(w, r, err)
			return
		}
		response.Success(w, r, resp)
	}
}

// IdleHeartbeatHandler 心跳上报
func IdleHeartbeatHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.IdleHeartbeatReq
		if err := httpx.Parse(r, &req); err != nil {
			response.BadRequest(w, r, "device_id is required")
			return
		}
		l := logic.NewIdleHeartbeatLogic(r.Context(), svcCtx)
		resp, err := l.IdleHeartbeat(&req)
		if err != nil {
			handleError(w, r, err)
			return
		}
		response.Success(w, r, resp)
	}
}

// IdleStopHandler 停止全部设备挂机
func IdleStopHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := logic.NewIdleStopLogic(r.Context(), svcCtx)
		resp, err := l.IdleStop()
		if err != nil {
			handleError(w, r, err)
			return
		}
		response.Success(w, r, resp)
	}
}

// IdleStopDeviceHandler 停止指定设备挂机
func IdleStopDeviceHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.IdleStopDeviceReq
		if err := httpx.Parse(r, &req); err != nil {
			response.BadRequest(w, r, "device_id is required")
			return
		}
		l := logic.NewIdleStopDeviceLogic(r.Context(), svcCtx)
		resp, err := l.IdleStopDevice(&req)
		if err != nil {
			handleError(w, r, err)
			return
		}
		response.Success(w, r, resp)
	}
}

// IdleStatusHandler 挂机状态
func IdleStatusHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := logic.NewIdleStatusLogic(r.Context(), svcCtx)
		resp, err := l.IdleStatus()
		if err != nil {
			handleError(w, r, err)
			return
		}
		response.Success(w, r, resp)
	}
}

// IdleRecordsHandler 挂机记录（分页）
func IdleRecordsHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.IdleRecordsReq
		if err := httpx.Parse(r, &req); err != nil {
			response.BadRequest(w, r, "invalid query params")
			return
		}
		l := logic.NewIdleRecordsLogic(r.Context(), svcCtx)
		items, nextCursor, hasMore, err := l.IdleRecords(&req)
		if err != nil {
			handleError(w, r, err)
			return
		}
		response.SuccessPage(w, r, items, formatCursor(nextCursor), hasMore)
	}
}

// formatCursor 空游标归一为 "0"（与 gin 版行为一致）
func formatCursor(c string) string {
	if c == "0" || c == "" {
		return "0"
	}
	return c
}
