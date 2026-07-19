package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cdcdx/hc-framework-go/pkg/contextkeys"
	"github.com/gin-gonic/gin"
)

func TestRequestInfo_InjectsClientIPAndUserAgent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(RequestInfo())
	var gotIP, gotUA string
	r.GET("/test", func(c *gin.Context) {
		gotIP, _ = c.Request.Context().Value(contextkeys.ClientIP).(string)
		gotUA, _ = c.Request.Context().Value(contextkeys.UserAgent).(string)
		c.Status(200)
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/test", nil)
	req.RemoteAddr = "192.168.1.1:12345"
	req.Header.Set("User-Agent", "test-agent/1.0")
	r.ServeHTTP(w, req)

	if gotIP != "192.168.1.1" {
		t.Errorf("ClientIP = %q, want 192.168.1.1", gotIP)
	}
	if gotUA != "test-agent/1.0" {
		t.Errorf("UserAgent = %q, want test-agent/1.0", gotUA)
	}
}
