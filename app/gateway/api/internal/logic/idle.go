package logic

import (
	"context"
	"strconv"

	"github.com/cdcdx/hc-framework-go/app/gateway/api/internal/svc"
	"github.com/cdcdx/hc-framework-go/app/gateway/api/internal/types"
	hcpb "github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/zeromicro/go-zero/core/logx"
)

// IdleStartLogic 开始挂机
type IdleStartLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewIdleStartLogic(ctx context.Context, svcCtx *svc.ServiceContext) *IdleStartLogic {
	return &IdleStartLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *IdleStartLogic) IdleStart(req *types.IdleStartReq) (any, error) {
	uid, ok := requireUser(l.ctx)
	if !ok {
		return nil, errUserUnauthorized
	}
	resp, err := l.svcCtx.HcRpc.IdleStart(l.ctx, &hcpb.IdleStartRequest{
		UserId:   uid,
		DeviceId: req.DeviceID,
	})
	if err != nil {
		return nil, err
	}
	return toData(resp), nil
}

// IdleHeartbeatLogic 心跳上报
type IdleHeartbeatLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewIdleHeartbeatLogic(ctx context.Context, svcCtx *svc.ServiceContext) *IdleHeartbeatLogic {
	return &IdleHeartbeatLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *IdleHeartbeatLogic) IdleHeartbeat(req *types.IdleHeartbeatReq) (any, error) {
	uid, ok := requireUser(l.ctx)
	if !ok {
		return nil, errUserUnauthorized
	}
	_, err := l.svcCtx.HcRpc.IdleHeartbeat(l.ctx, &hcpb.IdleHeartbeatRequest{
		UserId:   uid,
		DeviceId: req.DeviceID,
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{"status": "alive"}, nil
}

// IdleStopLogic 停止全部设备挂机
type IdleStopLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewIdleStopLogic(ctx context.Context, svcCtx *svc.ServiceContext) *IdleStopLogic {
	return &IdleStopLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *IdleStopLogic) IdleStop() (any, error) {
	uid, ok := requireUser(l.ctx)
	if !ok {
		return nil, errUserUnauthorized
	}
	resp, err := l.svcCtx.HcRpc.IdleStop(l.ctx, &hcpb.IdleStopRequest{UserId: uid})
	if err != nil {
		return nil, err
	}
	return toData(resp), nil
}

// IdleStopDeviceLogic 停止指定设备挂机
type IdleStopDeviceLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewIdleStopDeviceLogic(ctx context.Context, svcCtx *svc.ServiceContext) *IdleStopDeviceLogic {
	return &IdleStopDeviceLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *IdleStopDeviceLogic) IdleStopDevice(req *types.IdleStopDeviceReq) (any, error) {
	uid, ok := requireUser(l.ctx)
	if !ok {
		return nil, errUserUnauthorized
	}
	resp, err := l.svcCtx.HcRpc.IdleStopDevice(l.ctx, &hcpb.IdleStopDeviceRequest{
		UserId:   uid,
		DeviceId: req.DeviceID,
	})
	if err != nil {
		return nil, err
	}
	return toData(resp), nil
}

// IdleStatusLogic 挂机状态
type IdleStatusLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewIdleStatusLogic(ctx context.Context, svcCtx *svc.ServiceContext) *IdleStatusLogic {
	return &IdleStatusLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *IdleStatusLogic) IdleStatus() (any, error) {
	uid, ok := requireUser(l.ctx)
	if !ok {
		return nil, errUserUnauthorized
	}
	resp, err := l.svcCtx.HcRpc.IdleStatus(l.ctx, &hcpb.IdleStatusRequest{UserId: uid})
	if err != nil {
		return nil, err
	}
	return toData(resp), nil
}

// IdleRecordsLogic 挂机记录（分页）
type IdleRecordsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewIdleRecordsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *IdleRecordsLogic {
	return &IdleRecordsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *IdleRecordsLogic) IdleRecords(req *types.IdleRecordsReq) (items any, nextCursor string, hasMore bool, err error) {
	uid, ok := requireUser(l.ctx)
	if !ok {
		return nil, "", false, errUserUnauthorized
	}
	resp, err := l.svcCtx.HcRpc.IdleRecords(l.ctx, &hcpb.IdleRecordsRequest{
		UserId: uid,
		Cursor: req.Cursor,
		Limit:  int32(req.Limit),
	})
	if err != nil {
		return nil, "", false, err
	}
	return toDataList(resp.Records), strconv.FormatInt(resp.NextCursor, 10), resp.HasMore, nil
}
