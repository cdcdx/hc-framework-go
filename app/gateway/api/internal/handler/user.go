package handler

import (
	"net/http"

	"github.com/cdcdx/hc-framework-go/app/gateway/api/internal/logic"
	"github.com/cdcdx/hc-framework-go/app/gateway/api/internal/svc"
	"github.com/cdcdx/hc-framework-go/app/gateway/api/internal/types"
	"github.com/cdcdx/hc-framework-go/common/response"
	"github.com/zeromicro/go-zero/rest/httpx"
)

// GetProfileHandler 用户资料
func GetProfileHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := logic.NewGetProfileLogic(r.Context(), svcCtx)
		resp, err := l.GetProfile()
		if err != nil {
			handleError(w, r, err)
			return
		}
		response.Success(w, r, resp)
	}
}

// UpdateProfileHandler 更新用户资料
func UpdateProfileHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.UpdateProfileReq
		if err := httpx.Parse(r, &req); err != nil {
			response.BadRequest(w, r, "invalid request body")
			return
		}
		l := logic.NewUpdateProfileLogic(r.Context(), svcCtx)
		resp, err := l.UpdateProfile(&req)
		if err != nil {
			handleError(w, r, err)
			return
		}
		response.Success(w, r, resp)
	}
}

// GetPointsHandler 积分余额
func GetPointsHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := logic.NewGetPointsLogic(r.Context(), svcCtx)
		resp, err := l.GetPoints()
		if err != nil {
			handleError(w, r, err)
			return
		}
		response.Success(w, r, resp)
	}
}
