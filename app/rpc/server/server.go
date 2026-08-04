package server

import (
	"context"

	"github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/app/rpc/logic"
	"github.com/cdcdx/hc-framework-go/app/rpc/svc"
)

// HcServer 聚合所有领域 logic，实现 proto 定义的 HcServer 接口
type HcServer struct {
	hc.UnimplementedHcServer
	svcCtx *svc.ServiceContext
}

func NewHcServer(svcCtx *svc.ServiceContext) *HcServer {
	return &HcServer{svcCtx: svcCtx}
}

// ================= user 域 =================

func (s *HcServer) Register(ctx context.Context, in *hc.RegisterRequest) (*hc.RegisterResponse, error) {
	l := logic.NewRegisterLogic(ctx, s.svcCtx)
	return l.Register(in)
}

func (s *HcServer) Login(ctx context.Context, in *hc.LoginRequest) (*hc.LoginResponse, error) {
	l := logic.NewLoginLogic(ctx, s.svcCtx)
	return l.Login(in)
}

func (s *HcServer) GoogleOAuth(ctx context.Context, in *hc.GoogleOAuthRequest) (*hc.GoogleOAuthResponse, error) {
	l := logic.NewGoogleOAuthLogic(ctx, s.svcCtx)
	return l.GoogleOAuth(in)
}

func (s *HcServer) RefreshToken(ctx context.Context, in *hc.RefreshTokenRequest) (*hc.RefreshTokenResponse, error) {
	l := logic.NewRefreshTokenLogic(ctx, s.svcCtx)
	return l.RefreshToken(in)
}

func (s *HcServer) ChangePassword(ctx context.Context, in *hc.ChangePasswordRequest) (*hc.ChangePasswordResponse, error) {
	l := logic.NewChangePasswordLogic(ctx, s.svcCtx)
	return l.ChangePassword(in)
}

func (s *HcServer) GetProfile(ctx context.Context, in *hc.GetProfileRequest) (*hc.GetProfileResponse, error) {
	l := logic.NewGetProfileLogic(ctx, s.svcCtx)
	return l.GetProfile(in)
}

func (s *HcServer) UpdateProfile(ctx context.Context, in *hc.UpdateProfileRequest) (*hc.UpdateProfileResponse, error) {
	l := logic.NewUpdateProfileLogic(ctx, s.svcCtx)
	return l.UpdateProfile(in)
}

func (s *HcServer) GetPoints(ctx context.Context, in *hc.GetPointsRequest) (*hc.GetPointsResponse, error) {
	l := logic.NewGetPointsLogic(ctx, s.svcCtx)
	return l.GetPoints(in)
}

func (s *HcServer) AddPoints(ctx context.Context, in *hc.AddPointsRequest) (*hc.AddPointsResponse, error) {
	l := logic.NewAddPointsLogic(ctx, s.svcCtx)
	return l.AddPoints(in)
}

func (s *HcServer) DeductPoints(ctx context.Context, in *hc.DeductPointsRequest) (*hc.DeductPointsResponse, error) {
	l := logic.NewDeductPointsLogic(ctx, s.svcCtx)
	return l.DeductPoints(in)
}

// ================= idle 域 =================

func (s *HcServer) IdleStart(ctx context.Context, in *hc.IdleStartRequest) (*hc.IdleRecord, error) {
	l := logic.NewStartLogic(ctx, s.svcCtx)
	return l.Start(in)
}

func (s *HcServer) IdleHeartbeat(ctx context.Context, in *hc.IdleHeartbeatRequest) (*hc.Empty, error) {
	l := logic.NewHeartbeatLogic(ctx, s.svcCtx)
	return l.Heartbeat(in)
}

func (s *HcServer) IdleStop(ctx context.Context, in *hc.IdleStopRequest) (*hc.IdleRecord, error) {
	l := logic.NewStopLogic(ctx, s.svcCtx)
	return l.Stop(in)
}

func (s *HcServer) IdleStopDevice(ctx context.Context, in *hc.IdleStopDeviceRequest) (*hc.IdleRecord, error) {
	l := logic.NewStopDeviceLogic(ctx, s.svcCtx)
	return l.StopDevice(in)
}

func (s *HcServer) IdleStatus(ctx context.Context, in *hc.IdleStatusRequest) (*hc.IdleStatusResponse, error) {
	l := logic.NewStatusLogic(ctx, s.svcCtx)
	return l.Status(in)
}

func (s *HcServer) IdleRecords(ctx context.Context, in *hc.IdleRecordsRequest) (*hc.IdleRecordsResponse, error) {
	l := logic.NewRecordsLogic(ctx, s.svcCtx)
	return l.Records(in)
}

// ================= task 域 =================

func (s *HcServer) TaskList(ctx context.Context, in *hc.TaskListRequest) (*hc.TaskListResponse, error) {
	l := logic.NewListLogic(ctx, s.svcCtx)
	return l.List(in)
}

func (s *HcServer) TaskProgress(ctx context.Context, in *hc.TaskProgressRequest) (*hc.TaskProgressResponse, error) {
	l := logic.NewProgressLogic(ctx, s.svcCtx)
	return l.Progress(in)
}

func (s *HcServer) TaskClaim(ctx context.Context, in *hc.TaskClaimRequest) (*hc.TaskClaimResponse, error) {
	l := logic.NewClaimLogic(ctx, s.svcCtx)
	return l.Claim(in)
}

func (s *HcServer) ReportProgress(ctx context.Context, in *hc.ReportProgressRequest) (*hc.Empty, error) {
	l := logic.NewReportProgressLogic(ctx, s.svcCtx)
	return l.ReportProgress(in)
}

// ================= shop 域 =================

func (s *HcServer) ShopItems(ctx context.Context, in *hc.ShopItemsRequest) (*hc.ShopItemsResponse, error) {
	l := logic.NewItemsLogic(ctx, s.svcCtx)
	return l.Items(in)
}

func (s *HcServer) ShopRedeem(ctx context.Context, in *hc.ShopRedeemRequest) (*hc.RedeemResponse, error) {
	l := logic.NewRedeemLogic(ctx, s.svcCtx)
	return l.Redeem(in)
}

func (s *HcServer) ShopOrders(ctx context.Context, in *hc.ShopOrdersRequest) (*hc.ShopOrdersResponse, error) {
	l := logic.NewOrdersLogic(ctx, s.svcCtx)
	return l.Orders(in)
}

func (s *HcServer) ShopOrderDetail(ctx context.Context, in *hc.ShopOrderDetailRequest) (*hc.OrderInfo, error) {
	l := logic.NewOrderDetailLogic(ctx, s.svcCtx)
	return l.OrderDetail(in)
}

func (s *HcServer) FlashActivities(ctx context.Context, in *hc.FlashActivitiesRequest) (*hc.FlashActivitiesResponse, error) {
	l := logic.NewFlashActivitiesLogic(ctx, s.svcCtx)
	return l.FlashActivities(in)
}

func (s *HcServer) FlashRedeem(ctx context.Context, in *hc.FlashRedeemRequest) (*hc.RedeemResponse, error) {
	l := logic.NewFlashRedeemLogic(ctx, s.svcCtx)
	return l.FlashRedeem(in)
}
