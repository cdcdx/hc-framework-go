package handler

import (
	"net/http"

	"github.com/cdcdx/hc-framework-go/app/gateway/api/logic"
	"github.com/cdcdx/hc-framework-go/app/gateway/api/svc"
	"github.com/cdcdx/hc-framework-go/app/gateway/api/types"
	"github.com/cdcdx/hc-framework-go/common/response"
	"github.com/zeromicro/go-zero/rest/httpx"
)

// TaskListHandler 任务列表
func TaskListHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := logic.NewTaskListLogic(r.Context(), svcCtx)
		resp, err := l.TaskList()
		if err != nil {
			handleError(w, r, err)
			return
		}
		response.Success(w, r, resp)
	}
}

// TaskProgressHandler 任务进度
func TaskProgressHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := logic.NewTaskProgressLogic(r.Context(), svcCtx)
		resp, err := l.TaskProgress()
		if err != nil {
			handleError(w, r, err)
			return
		}
		response.Success(w, r, resp)
	}
}

// TaskClaimHandler 领取任务奖励
func TaskClaimHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.TaskClaimReq
		if err := httpx.Parse(r, &req); err != nil {
			response.BadRequest(w, r, "invalid task id")
			return
		}
		l := logic.NewTaskClaimLogic(r.Context(), svcCtx)
		resp, err := l.TaskClaim(&req)
		if err != nil {
			handleError(w, r, err)
			return
		}
		response.Success(w, r, resp)
	}
}
