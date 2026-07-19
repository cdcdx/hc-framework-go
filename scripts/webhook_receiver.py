#!/usr/bin/env python3
"""Alertmanager 本地 webhook 接收器（仅标准库，无第三方依赖）。

把 Alertmanager 发来的告警写入日志文件并打印，便于在本地验证「Prometheus 告警 ->
Alertmanager 路由 -> webhook 送达」整条链路，无需接入 Slack/PagerDuty 等外部系统。

路由：
  POST /alerts   默认接收方（default-webhook）
  POST /flash    flash 容量告警接收方（flash-capacity-pager）

运行：
  python3 scripts/webhook_receiver.py            # 监听 :9095
  python3 scripts/webhook_receiver.py 9096       # 指定端口

日志：/tmp/alertmanager_webhook.log
"""
import json
import sys
import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

LOG_PATH = "/tmp/alertmanager_webhook.log"


class Handler(BaseHTTPRequestHandler):
    def _handle(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            data = json.loads(raw)
        except Exception:
            data = {"raw": raw.decode("utf-8", "replace")}
        ts = datetime.datetime.now().isoformat(timespec="seconds")
        alerts = data.get("alerts", []) if isinstance(data, dict) else []
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(f"[{ts}] {self.path} alerts={len(alerts)}\n")
            f.write(json.dumps(data, ensure_ascii=False, indent=2) + "\n")
            f.write("-" * 60 + "\n")
        print(f"[{ts}] {self.path} alerts={len(alerts)}", flush=True)
        body = json.dumps({"status": "ok"}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        self._handle()

    def log_message(self, *args):  # 静默默认访问日志
        pass


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9095
    print(f"webhook receiver listening on :{port} -> {LOG_PATH}", flush=True)
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()
