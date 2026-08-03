package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/cdcdx/hc-framework-go/app/user/rpc/internal/svc"
	"github.com/cdcdx/hc-framework-go/app/user/rpc/user"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/google/uuid"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

// GoogleOAuthLogic Google OAuth 登录/注册
type GoogleOAuthLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGoogleOAuthLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GoogleOAuthLogic {
	return &GoogleOAuthLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

type googleTokenResp struct {
	AccessToken string `json:"access_token"`
	IDToken     string `json:"id_token"`
	Error       string `json:"error"`
	ErrorDesc   string `json:"error_description"`
}

type googleUserInfo struct {
	ID      string `json:"id"`
	Email   string `json:"email"`
	Name    string `json:"name"`
	Picture string `json:"picture"`
}

func (l *GoogleOAuthLogic) GoogleOAuth(in *user.GoogleOAuthRequest) (*user.GoogleOAuthResponse, error) {
	if in.Code == "" {
		return nil, errorx.New(errorx.CodeInvalidParam, "code is required")
	}

	// 1. 授权码换 access_token（client 凭据来自配置，见 svc 扩展项）
	cfg := l.svcCtx.Config
	clientID := cfg.GoogleOAuth.ClientID
	clientSecret := cfg.GoogleOAuth.ClientSecret
	redirectURI := cfg.GoogleOAuth.RedirectURI
	if clientID == "" || clientSecret == "" {
		return nil, errorx.New(errorx.CodeThirdPartyError, "google oauth not configured")
	}

	form := url.Values{}
	form.Set("code", in.Code)
	form.Set("client_id", clientID)
	form.Set("client_secret", clientSecret)
	form.Set("redirect_uri", redirectURI)
	form.Set("grant_type", "authorization_code")

	httpClient := &http.Client{Timeout: 10 * time.Second}
	resp, err := httpClient.PostForm("https://oauth2.googleapis.com/token", form)
	if err != nil {
		return nil, errorx.New(errorx.CodeThirdPartyError, err.Error())
	}
	defer resp.Body.Close()
	tokenBody, _ := io.ReadAll(resp.Body)
	var tr googleTokenResp
	if err := json.Unmarshal(tokenBody, &tr); err != nil || tr.AccessToken == "" {
		return nil, errorx.New(errorx.CodeThirdPartyError, "google token exchange failed: "+tr.ErrorDesc)
	}

	// 2. 拉取用户信息
	req, _ := http.NewRequestWithContext(l.ctx,
		http.MethodGet, "https://www.googleapis.com/oauth2/v2/userinfo", nil)
	req.Header.Set("Authorization", "Bearer "+tr.AccessToken)
	infoResp, err := httpClient.Do(req)
	if err != nil {
		return nil, errorx.New(errorx.CodeThirdPartyError, err.Error())
	}
	defer infoResp.Body.Close()
	infoBody, _ := io.ReadAll(infoResp.Body)
	var info googleUserInfo
	if err := json.Unmarshal(infoBody, &info); err != nil || info.ID == "" {
		return nil, errorx.New(errorx.CodeThirdPartyError, "google userinfo fetch failed")
	}

	// 3. 按 google_id 或 email 查找用户，不存在则自动注册
	u, err := l.findOrCreate(info)
	if err != nil {
		return nil, err
	}

	access, refresh, err := l.svcCtx.JwtMgr.GenerateTokenPair(u.UserID, u.Email)
	if err != nil {
		return nil, errorx.New(errorx.CodeUnknownError, err.Error())
	}

	return &user.GoogleOAuthResponse{
		User:         toUserInfo(u),
		AccessToken:  access,
		RefreshToken: refresh,
	}, nil
}

func (l *GoogleOAuthLogic) findOrCreate(info googleUserInfo) (*model.User, error) {
	var u model.User
	err := l.svcCtx.Db.Where("google_id = ?", info.ID).First(&u).Error
	if err == nil {
		if u.Status != "active" {
			return nil, errorx.New(errorx.CodeAccountLocked)
		}
		return &u, nil
	}
	if err != gorm.ErrRecordNotFound {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}

	// 兜底：同 email 已注册（非 Google 方式）→ 绑定 google_id
	err = l.svcCtx.Db.Where("email = ?", info.Email).First(&u).Error
	if err == nil {
		if u.Status != "active" {
			return nil, errorx.New(errorx.CodeAccountLocked)
		}
		if err := l.svcCtx.Db.Model(&u).UpdateColumn("google_id", info.ID).Error; err != nil {
			return nil, errorx.New(errorx.CodeDBError, err.Error())
		}
		return &u, nil
	}
	if err != gorm.ErrRecordNotFound {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}

	// 全新用户自动注册
	name := info.Name
	if name == "" {
		name = info.Email
	}
	nu := &model.User{
		UserID:   uuid.NewString(),
		Username: name,
		Email:    info.Email,
		GoogleID: &info.ID,
		AvatarURL: info.Picture,
		Status:   "active",
	}
	if err := l.svcCtx.Db.Create(nu).Error; err != nil {
		return nil, fmt.Errorf("create user: %w", err)
	}
	return nu, nil
}
