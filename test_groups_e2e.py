"""分组路由端到端测试：组选择、组内策略隔离（rotation 按组独立窗口）、
流式转发、管理接口、向后兼容。

运行：python test_groups_e2e.py（自起 echo 与代理，结束后自动清理）。
注意：请先停掉正在运行的代理实例，避免端口/进程混淆。
"""
import json
import os
import subprocess
import sys
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

UP_PORT = 9901
PROXY_PORT = 8799
UP_BASE = f"http://127.0.0.1:{UP_PORT}"
PROXY_BASE = f"http://127.0.0.1:{PROXY_PORT}"

FAILURES = []


def check(name, cond, detail=""):
    mark = "PASS" if cond else "FAIL"
    print(f"[{mark}] {name}" + (f" | {detail}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


class Echo(BaseHTTPRequestHandler):
    """假上游：回显服务的 Key 尾号与收到的路径；body 带 stream=true 时回 SSE。"""

    protocol_version = "HTTP/1.1"

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(n)
        tail = self.headers.get("Authorization", "")[-4:]
        try:
            wants_stream = bool(json.loads(raw.decode("utf-8")).get("stream"))
        except (ValueError, UnicodeDecodeError):
            wants_stream = False

        if wants_stream:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Connection", "close")
            self.end_headers()
            for i in range(3):
                chunk = json.dumps({"served": tail, "i": i})
                self.wfile.write(f"data: {chunk}\n\n".encode())
                self.wfile.flush()
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
            self.close_connection = True
            return

        data = json.dumps({"served": tail, "upstream_path": self.path}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *a):
        pass


def post(path, body=None):
    body = body or b'{"model":"m"}'
    req = urllib.request.Request(PROXY_BASE + path, data=body,
                                 headers={"Content-Type": "application/json"}, method="POST")
    return json.loads(urllib.request.urlopen(req, timeout=15).read())


def main():
    srv = ThreadingHTTPServer(("127.0.0.1", UP_PORT), Echo)
    threading.Thread(target=srv.serve_forever, daemon=True).start()

    logf = open("proxy_groups_e2e.log", "w")
    env = dict(os.environ,
               KEYPOOL_KEYS=(f"ag-1aaaa|{UP_BASE}/api/plan/v3|ark-plan,"
                             f"ag-2bbbb|{UP_BASE}/api/v3|gB,"
                             f"ag-3cccc|{UP_BASE}/api/v3|gB,"
                             f"ag-4dddd|{UP_BASE}/api/v3,sk-5eeee|{UP_BASE}/v1|docode_cc,sk-6ffff|{UP_BASE}/v1|docode_gc,sk-7gggg|{UP_BASE}/v1|docode_gpt,"
),
               PROXY_PORT=str(PROXY_PORT), PROXY_HOST="127.0.0.1",
               KEY_PICK_STRATEGY="rotation", ROTATION_WINDOW_REQUESTS="2",
               LOG_LEVEL="WARNING")
    p = subprocess.Popen([sys.executable, "proxy_server.py"], env=env,
                         stdout=logf, stderr=logf,
                         cwd=os.path.dirname(os.path.abspath(__file__)))
    time.sleep(1.5)

    try:
        # 1) 组内 rotation 按组独立：gB 窗口=2；中间插入的 ark-plan 请求不干扰 gB 计数
        seq = [post("/gB/api/v3/chat/completions")["served"],
               post("/gB/api/v3/chat/completions")["served"],
               post("/ark-plan/chat/completions")["served"],
               post("/gB/api/v3/chat/completions")["served"],
               post("/gB/api/v3/chat/completions")["served"]]
        check("组内 rotation 独立：gB 窗口不被其他组请求打断",
              seq == ["bbbb", "bbbb", "aaaa", "cccc", "cccc"], str(seq))

        # 2) 干净式：组名 + 端点后缀 → Key 地址 + 后缀
        r = post("/ark-plan/chat/completions")
        check("干净式路由（组名 + /chat/completions）",
              r["served"] == "aaaa" and r["upstream_path"] == "/api/plan/v3/chat/completions")

        # 3) 镜像式：组名 + 上游路径前缀（兼容旧配置），前缀被剥离
        r = post("/ark-plan/api/plan/v3/chat/completions")
        check("镜像式路由（前缀剥离）",
              r["served"] == "aaaa" and r["upstream_path"] == "/api/plan/v3/chat/completions")

        # 4) default 组：未打标 Key + 无组名请求（向后兼容）
        r = post("/api/v3/chat/completions")
        check("default 组镜像式（向后兼容）",
              r["served"] == "dddd" and r["upstream_path"] == "/api/v3/chat/completions")
        r = post("/chat/completions")
        check("default 组干净式", r["served"] == "dddd")

        # 5) 流式转发经分组路由：SSE 块原样到达且由组内 Key 服务
        req = urllib.request.Request(
            PROXY_BASE + "/gB/api/v3/chat/completions",
            data=json.dumps({"model": "m", "stream": True}).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        resp = urllib.request.urlopen(req, timeout=15)
        sse = resp.read().decode("utf-8")
        served_tails = {json.loads(line[5:])["served"] for line in sse.splitlines()
                        if line.startswith("data: ") and "[DONE]" not in line}
        check("流式经分组转发（SSE 完整到达）",
              sse.count("data:") >= 3 and "[DONE]" in sse and served_tails <= {"bbbb", "cccc"})

        # 6) __pool 查询参数选组
        r = post("/api/v3/chat/completions?__pool=ark-plan")
        check("__pool 查询参数选组", r["served"] == "aaaa")

        # 7) 用户新增分组：裸组名 + SDK 端点后缀；每组打到自己 Key 地址
        r = post("/docode_cc/chat/completions")
        check("Docode Claude 组干净路由",
              r["served"] == "eeee" and r["upstream_path"] == "/v1/chat/completions")
        r = post("/docode_gc/chat/completions")
        check("Docode DeepSeek 组干净路由",
              r["served"] == "ffff" and r["upstream_path"] == "/v1/chat/completions")
        # 组之间隔离：不带组名仍只走 default 的 dddd，不会误选两个 Docode key
        r = post("/api/v3/chat/completions")
        check("Docode 组不影响 default", r["served"] == "dddd")

        # 8) 裸组路径返回组概要
        info = json.loads(urllib.request.urlopen(PROXY_BASE + "/docode_cc", timeout=10).read())
        check("Docode 裸组路径 → 组概要",
              info["group"] == "docode_cc" and info["total"] == 1 and info["usable"] == 1)

        # 8) 真实新增分组的 API 约定：三个服务组都用 /v1 端点（上游 /models 是 HTML 控制台，
        # OpenAI 兼容模型列表应使用 /v1/models）。本地 echo 验证路径拼接。
        for group in ("docode_cc", "docode_gc", "docode_gpt"):
            r = post(f"/{group}/v1/models")
            expected_tail = {"docode_cc": "eeee", "docode_gc": "ffff", "docode_gpt": "gggg"}[group]
            check(f"{group} clean /v1 route", r["served"] == expected_tail,
                  f"served={r['served']} expected={expected_tail}")

        # 9) 管理接口
        st = json.loads(urllib.request.urlopen(PROXY_BASE + "/pool/status", timeout=10).read())
        check("/pool/status 含全部分组视图与 rotation 窗口",
              set(st["groups"]) == {"ark-plan", "gB", "default", "docode_cc", "docode_gc", "docode_gpt"}
              and "active_key" in st["groups"]["gB"])
        rec_req = urllib.request.Request(PROXY_BASE + "/pool/recover", data=b"{}", method="POST")
        rec = json.loads(urllib.request.urlopen(rec_req, timeout=10).read())
        check("/pool/recover 返回状态", "recovered" in rec)
    finally:
        p.terminate()
        srv.shutdown()

    print("=" * 40)
    print("ALL PASS" if not FAILURES else f"FAILED: {FAILURES}")
    sys.exit(1 if FAILURES else 0)


if __name__ == "__main__":
    main()
