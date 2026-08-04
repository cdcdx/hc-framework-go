package errorx

import (
	"errors"
	"testing"
)

func TestNew_CodeMsgRoundTrip(t *testing.T) {
	tests := []struct {
		code int
		msg  string
	}{
		{CodeSuccess, "success"},
		{CodeInvalidParam, "参数校验失败"},
		{CodeTokenExpired, "Token 已过期"},
		{CodePointsInsufficient, "积分不足"},
		{CodeDailyPointsLimit, "已达每日积分上限"},
		{CodeDBError, "数据库异常"},
	}
	for _, tt := range tests {
		err := New(tt.code)
		if Code(err) != tt.code {
			t.Errorf("Code(New(%d)) = %d, want %d", tt.code, Code(err), tt.code)
		}
		if Msg(err) != tt.msg {
			t.Errorf("Msg(New(%d)) = %q, want %q", tt.code, Msg(err), tt.msg)
		}
	}
}

func TestNew_CustomMessage(t *testing.T) {
	err := New(CodePasswordWrong, "密码错误啦")
	if Code(err) != CodePasswordWrong {
		t.Errorf("Code = %d, want %d", Code(err), CodePasswordWrong)
	}
	if Msg(err) != "密码错误啦" {
		t.Errorf("Msg = %q, want 密码错误啦", Msg(err))
	}
}

func TestNewf_Formatting(t *testing.T) {
	err := Newf(CodeRateLimited, "IP %s 限流", "1.2.3.4")
	if Msg(err) != "IP 1.2.3.4 限流" {
		t.Errorf("Msg = %q, want formatted", Msg(err))
	}
	if Code(err) != CodeRateLimited {
		t.Errorf("Code = %d, want %d", Code(err), CodeRateLimited)
	}
}

func TestCode_NilAndPlainError(t *testing.T) {
	if Code(nil) != CodeSuccess {
		t.Errorf("Code(nil) = %d, want %d", Code(nil), CodeSuccess)
	}
	// 普通 error 无 status 信息，应回退到 CodeUnknownError。
	if Code(errors.New("boom")) != CodeUnknownError {
		t.Errorf("Code(plain) = %d, want %d", Code(errors.New("boom")), CodeUnknownError)
	}
	if Msg(errors.New("boom")) != Message(CodeUnknownError) {
		t.Errorf("Msg(plain) mismatch")
	}
}

func TestMessage_Fallback(t *testing.T) {
	if Message(999999) != "未知错误" {
		t.Errorf("Message(unknown) = %q, want 未知错误", Message(999999))
	}
}

func TestErrorMessages_AllCodesHaveMessage(t *testing.T) {
	codes := []int{
		CodeSuccess, CodeInvalidParam, CodeNotFound, CodeGatewayTimeout, CodeServiceUnavailable,
		CodeTokenExpired, CodeTokenInvalid, CodePermissionDenied, CodeTokenBlacklisted,
		CodeEmailRegistered, CodePasswordWrong, CodeAccountLocked, CodeAccountDisabled, CodeCaptchaRequired,
		CodePointsInsufficient, CodeStockInsufficient, CodeItemOffline, CodeDuplicateRedeem,
		CodeFlashSaleNotStarted, CodeFlashSaleEnded, CodeFlashSaleSoldOut, CodeFlashSaleUserLimit,
		CodeFlashSaleTimeout, CodeRedeemConcurrent, CodeRedeemSoldOutPeak,
		CodeAlreadyIdle, CodeNotIdle, CodeKickedOffline, CodeHeartbeatTimeout, CodeDailyPointsLimit,
		CodeTaskNotCompleted, CodeTaskClaimed, CodeTaskExpired, CodeProgressInsufficient,
		CodeRateLimited, CodeCircuitOpen, CodeCaptchaVerify,
		CodeDBError, CodeRedisError, CodeKafkaError, CodeThirdPartyError,
	}
	for _, c := range codes {
		if Message(c) == "" {
			t.Errorf("code %d has empty message", c)
		}
		if Message(c) == "未知错误" {
			t.Errorf("code %d falls back to 未知错误 (missing mapping)", c)
		}
	}
	// CodeUnknownError 的消息本就是 "未知错误"，单独验证其映射存在且正确。
	if Message(CodeUnknownError) != "未知错误" {
		t.Errorf("Message(CodeUnknownError) = %q, want 未知错误", Message(CodeUnknownError))
	}
}
