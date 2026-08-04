package server

import (
	"context"

	"github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"google.golang.org/grpc"
)

// LocalHcClient 进程内 Hc 客户端，实现 hc.Hc 接口（与跨进程 gRPC client 同签名）。
// 合并部署时，gateway 不再创建 gRPC client，而是直接持有本结构体，
// 把调用转为对 *HcServer 的方法调用，省去网络与服务发现开销。
type LocalHcClient struct {
	s *HcServer
}

// NewLocalHcClient 由 *HcServer 构造进程内 client。
func NewLocalHcClient(s *HcServer) *LocalHcClient {
	return &LocalHcClient{s: s}
}

func (c *LocalHcClient) Register(ctx context.Context, in *hc.RegisterRequest, _ ...grpc.CallOption) (*hc.RegisterResponse, error) {
	return c.s.Register(ctx, in)
}

func (c *LocalHcClient) Login(ctx context.Context, in *hc.LoginRequest, _ ...grpc.CallOption) (*hc.LoginResponse, error) {
	return c.s.Login(ctx, in)
}

func (c *LocalHcClient) GoogleOAuth(ctx context.Context, in *hc.GoogleOAuthRequest, _ ...grpc.CallOption) (*hc.GoogleOAuthResponse, error) {
	return c.s.GoogleOAuth(ctx, in)
}

func (c *LocalHcClient) RefreshToken(ctx context.Context, in *hc.RefreshTokenRequest, _ ...grpc.CallOption) (*hc.RefreshTokenResponse, error) {
	return c.s.RefreshToken(ctx, in)
}

func (c *LocalHcClient) ChangePassword(ctx context.Context, in *hc.ChangePasswordRequest, _ ...grpc.CallOption) (*hc.ChangePasswordResponse, error) {
	return c.s.ChangePassword(ctx, in)
}

func (c *LocalHcClient) GetProfile(ctx context.Context, in *hc.GetProfileRequest, _ ...grpc.CallOption) (*hc.GetProfileResponse, error) {
	return c.s.GetProfile(ctx, in)
}

func (c *LocalHcClient) UpdateProfile(ctx context.Context, in *hc.UpdateProfileRequest, _ ...grpc.CallOption) (*hc.UpdateProfileResponse, error) {
	return c.s.UpdateProfile(ctx, in)
}

func (c *LocalHcClient) GetPoints(ctx context.Context, in *hc.GetPointsRequest, _ ...grpc.CallOption) (*hc.GetPointsResponse, error) {
	return c.s.GetPoints(ctx, in)
}

func (c *LocalHcClient) AddPoints(ctx context.Context, in *hc.AddPointsRequest, _ ...grpc.CallOption) (*hc.AddPointsResponse, error) {
	return c.s.AddPoints(ctx, in)
}

func (c *LocalHcClient) DeductPoints(ctx context.Context, in *hc.DeductPointsRequest, _ ...grpc.CallOption) (*hc.DeductPointsResponse, error) {
	return c.s.DeductPoints(ctx, in)
}

func (c *LocalHcClient) IdleStart(ctx context.Context, in *hc.IdleStartRequest, _ ...grpc.CallOption) (*hc.IdleRecord, error) {
	return c.s.IdleStart(ctx, in)
}

func (c *LocalHcClient) IdleHeartbeat(ctx context.Context, in *hc.IdleHeartbeatRequest, _ ...grpc.CallOption) (*hc.Empty, error) {
	return c.s.IdleHeartbeat(ctx, in)
}

func (c *LocalHcClient) IdleStop(ctx context.Context, in *hc.IdleStopRequest, _ ...grpc.CallOption) (*hc.IdleRecord, error) {
	return c.s.IdleStop(ctx, in)
}

func (c *LocalHcClient) IdleStopDevice(ctx context.Context, in *hc.IdleStopDeviceRequest, _ ...grpc.CallOption) (*hc.IdleRecord, error) {
	return c.s.IdleStopDevice(ctx, in)
}

func (c *LocalHcClient) IdleStatus(ctx context.Context, in *hc.IdleStatusRequest, _ ...grpc.CallOption) (*hc.IdleStatusResponse, error) {
	return c.s.IdleStatus(ctx, in)
}

func (c *LocalHcClient) IdleRecords(ctx context.Context, in *hc.IdleRecordsRequest, _ ...grpc.CallOption) (*hc.IdleRecordsResponse, error) {
	return c.s.IdleRecords(ctx, in)
}

func (c *LocalHcClient) TaskList(ctx context.Context, in *hc.TaskListRequest, _ ...grpc.CallOption) (*hc.TaskListResponse, error) {
	return c.s.TaskList(ctx, in)
}

func (c *LocalHcClient) TaskProgress(ctx context.Context, in *hc.TaskProgressRequest, _ ...grpc.CallOption) (*hc.TaskProgressResponse, error) {
	return c.s.TaskProgress(ctx, in)
}

func (c *LocalHcClient) TaskClaim(ctx context.Context, in *hc.TaskClaimRequest, _ ...grpc.CallOption) (*hc.TaskClaimResponse, error) {
	return c.s.TaskClaim(ctx, in)
}

func (c *LocalHcClient) ReportProgress(ctx context.Context, in *hc.ReportProgressRequest, _ ...grpc.CallOption) (*hc.Empty, error) {
	return c.s.ReportProgress(ctx, in)
}

func (c *LocalHcClient) ShopItems(ctx context.Context, in *hc.ShopItemsRequest, _ ...grpc.CallOption) (*hc.ShopItemsResponse, error) {
	return c.s.ShopItems(ctx, in)
}

func (c *LocalHcClient) ShopRedeem(ctx context.Context, in *hc.ShopRedeemRequest, _ ...grpc.CallOption) (*hc.RedeemResponse, error) {
	return c.s.ShopRedeem(ctx, in)
}

func (c *LocalHcClient) ShopOrders(ctx context.Context, in *hc.ShopOrdersRequest, _ ...grpc.CallOption) (*hc.ShopOrdersResponse, error) {
	return c.s.ShopOrders(ctx, in)
}

func (c *LocalHcClient) ShopOrderDetail(ctx context.Context, in *hc.ShopOrderDetailRequest, _ ...grpc.CallOption) (*hc.OrderInfo, error) {
	return c.s.ShopOrderDetail(ctx, in)
}

func (c *LocalHcClient) FlashActivities(ctx context.Context, in *hc.FlashActivitiesRequest, _ ...grpc.CallOption) (*hc.FlashActivitiesResponse, error) {
	return c.s.FlashActivities(ctx, in)
}

func (c *LocalHcClient) FlashRedeem(ctx context.Context, in *hc.FlashRedeemRequest, _ ...grpc.CallOption) (*hc.RedeemResponse, error) {
	return c.s.FlashRedeem(ctx, in)
}
