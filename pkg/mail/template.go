package mail

import (
	"bytes"
	"fmt"
	"html/template"
)

// confirmRegisterTmpl 注册确认邮件 HTML 模板。数据字段见 ConfirmRegisterData。
var confirmRegisterTmpl = template.Must(template.New("confirm-register").Parse(`<!DOCTYPE html>
<html lang="zh-CN">
<head><meta charset="UTF-8"><title>{{.Subject}}</title></head>
<body style="font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;background:#f5f5f5;padding:24px;">
  <div style="max-width:480px;margin:0 auto;background:#fff;border-radius:12px;padding:32px;">
    <h2 style="color:#1a1a1a;">欢迎加入，{{.Username}}！</h2>
    <p style="color:#555;line-height:1.6;">感谢你注册 {{.SiteName}}。你的账号已成功创建：</p>
    <p style="color:#555;line-height:1.6;">邮箱：<strong>{{.Email}}</strong></p>
    <p style="color:#555;line-height:1.6;">注册时间：{{.RegisterTime}}</p>
    <p style="color:#999;line-height:1.6;font-size:13px;">如果这不是你本人的操作，请忽略此邮件，账号不会因此被任何方式激活或变更。</p>
    <hr style="border:none;border-top:1px solid #eee;margin:24px 0;" />
    <p style="color:#aaa;font-size:12px;">本邮件由系统自动发送，请勿直接回复。</p>
  </div>
</body>
</html>`))

// ConfirmRegisterData 注册确认邮件模板数据。
type ConfirmRegisterData struct {
	SiteName     string
	Username     string
	Email        string
	RegisterTime string
	Subject      string
}

// BuildConfirmRegisterHTML 渲染注册确认邮件 HTML 正文。
func BuildConfirmRegisterHTML(d ConfirmRegisterData) (string, error) {
	var buf bytes.Buffer
	if err := confirmRegisterTmpl.Execute(&buf, d); err != nil {
		return "", fmt.Errorf("mail: render confirm template: %w", err)
	}
	return buf.String(), nil
}
