"""大请求体 / 多模态 / 大响应 专项传输测试。

自建 echo 上游（9903），代理（8788）转发，验证：
T1 小 JSON 基线
T2 5MB  base64 图片型 JSON
T3 60MB base64 视频型 JSON（测耗时与代理内存）
T4 multipart/form-data 1MB 二进制（音频上传类）
T5 chunked 请求体（无 Content-Length，Node 流式上传场景）
T6 100MB SSE 流式响应（测代理内存是否恒定）
T7 25MB 非流式大响应
T8 客户端中途断流后代理是否存活

运行：python test_large_payload.py
"""
import base64
import http.client
import json
import os
import re
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

UP_PORT = 9903
PROXY_PORT = 8788
PROXY_BASE = f"http://127.0.0.1:{PROXY_PORT}"
UP_BASE = f"http://127.0.0.1:{UP_PORT}/api/plan/v3"


class EchoHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self):  # noqa: N802
        if self.path.endswith("/big-json"):
            data = b'{"pad":"' + b"x" * 25_000_000 + b'"}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        if self.path.endswith("/big-sse"):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            line = b'data: {"pad":"' + b"y" * 60_000 + b'"}\n\n'
            sent = 0
            while sent < 100 * 1024 * 1024:
                self.wfile.write(line)
                sent += len(line)
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
            self.close_connection = True
            return
        try:
            n = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            n = 0
        body = self.rfile.read(n) if n else b""
        payload = json.dumps({
            "received_bytes": len(body),
            "content_type": self.headers.get("Content-Type", ""),
            "head_hex": body[:8].hex(),
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *a):  # noqa: N802
        pass


def rss_mb(pid: int) -> float:
    out = subprocess.check_output(
        ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
        text=True, errors="replace",
    )
    m = re.findall(r'"\s*([\d\.,]+)\s*K\s*"', out)
    if not m:
        return -1
    return float(re.sub(r"[^\d]", "", m[-1]))


def post(path: str, body: bytes, content_type: str = "application/json"):
    req = urllib.request.Request(
        PROXY_BASE + path, data=body,
        headers={"Content-Type": content_type}, method="POST",
    )
    t0 = time.time()
    resp = urllib.request.urlopen(req, timeout=600)
    data = resp.read()
    return resp.status, json.loads(data), time.time() - t0


def main() -> None:
    upstream = ThreadingHTTPServer(("127.0.0.1", UP_PORT), EchoHandler)
    threading.Thread(target=upstream.serve_forever, daemon=True).start()

    logf = open("proxy_large.log", "w", encoding="utf-8")
    env = dict(os.environ,
               KEYPOOL_KEYS=f"testkey-t123|{UP_BASE}",
               PROXY_PORT=str(PROXY_PORT),
               LOG_LEVEL="WARNING")
    proxy = subprocess.Popen([sys.executable, "proxy_server.py"],
                             env=env, stdout=logf, stderr=logf,
                             cwd=os.path.dirname(os.path.abspath(__file__)))
    for _ in range(50):
        try:
            urllib.request.urlopen(f"{PROXY_BASE}/pool/status", timeout=1).read()
            break
        except OSError:
            time.sleep(0.2)
    pid = proxy.pid
    print(f"proxy pid={pid}, baseline RSS={rss_mb(pid):.0f} MB")

    # T1 基线
    st, d, _ = post("/api/plan/v3/chat/completions",
                    json.dumps({"model": "m", "messages": []}).encode())
    print(f"[T1] small json: {st}, upstream got {d['received_bytes']}B")

    # T2 5MB base64 图片
    img = base64.b64encode(os.urandom(3_750_000)).decode()
    body = json.dumps({"model": "m", "messages": [{"role": "user", "content": [
        {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{img}"}}]}]}).encode()
    st, d, dt = post("/api/plan/v3/chat/completions", body)
    print(f"[T2] 5MB image json: {st}, {len(body)}B sent, upstream got {d['received_bytes']}B, {dt:.2f}s")

    # T3 60MB base64 视频
    vid = base64.b64encode(os.urandom(45_000_000)).decode()
    body = json.dumps({"model": "m", "messages": [{"role": "user", "content": [
        {"type": "video_url", "video_url": {"url": f"data:video/mp4;base64,{vid}"}}]}]}).encode()
    rss0 = rss_mb(pid)
    st, d, dt = post("/api/plan/v3/chat/completions", body)
    print(f"[T3] 60MB video json: {st}, {len(body)/1e6:.0f}MB sent, upstream got {d['received_bytes']/1e6:.0f}MB, {dt:.2f}s, proxy RSS {rss0:.0f}->{rss_mb(pid):.0f}MB")

    # T4 multipart 1MB
    boundary = "----testbnd123"
    filebytes = os.urandom(1_000_000)
    mp = (f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"a.wav\"\r\n"
          f"Content-Type: audio/wav\r\n\r\n").encode() + filebytes + f"\r\n--{boundary}--\r\n".encode()
    st, d, _ = post("/api/plan/v3/audio/transcriptions", mp, f"multipart/form-data; boundary={boundary}")
    print(f"[T4] multipart 1MB: {st}, upstream got {d['received_bytes']}B, ct={d['content_type'][:40]}...")

    # T5 chunked 请求体
    try:
        conn = http.client.HTTPConnection("127.0.0.1", PROXY_PORT, timeout=60)
        conn.request("POST", "/api/plan/v3/chat/completions",
                     body=iter([b'{"model":"m","mess', b'ages":[]}']),
                     headers={"Content-Type": "application/json"}, encode_chunked=True)
        r = conn.getresponse()
        d = json.loads(r.read())
        verdict = "OK" if d["received_bytes"] == 27 else f"SILENT LOSS: upstream got {d['received_bytes']}B (expect 27)"
        print(f"[T5] chunked request body: {r.status}, {verdict}")
        conn.close()
    except Exception as e:
        print(f"[T5] chunked request body: EXCEPTION {e}")

    # T6 100MB SSE
    rss0 = rss_mb(pid)
    req = urllib.request.Request(PROXY_BASE + "/api/plan/v3/big-sse",
                                 data=b'{"stream":true}',
                                 headers={"Content-Type": "application/json"}, method="POST")
    t0 = time.time()
    resp = urllib.request.urlopen(req, timeout=600)
    total = 0
    deadline = time.time() + 300
    while time.time() < deadline:
        c = resp.read1(1 << 16)
        if not c:
            break
        total += len(c)
        if total >= 100 * 1024 * 1024:
            break
    print(f"[T6] 100MB SSE via proxy: client got {total/1e6:.0f}MB in {time.time()-t0:.1f}s, proxy RSS {rss0:.0f}->{rss_mb(pid):.0f}MB")

    # T7 25MB 非流式响应（上游 /big-json 返回 25MB JSON）
    rss0 = rss_mb(pid)
    req = urllib.request.Request(PROXY_BASE + "/api/plan/v3/big-json", data=b'{"stream":false}',
                                 headers={"Content-Type": "application/json"}, method="POST")
    t0 = time.time()
    resp = urllib.request.urlopen(req, timeout=600)
    big = resp.read()
    print(f"[T7] 25MB non-stream response: got {len(big)/1e6:.0f}MB in {time.time()-t0:.1f}s, proxy RSS {rss0:.0f}->{rss_mb(pid):.0f}MB")

    # T8 客户端中途断流
    s = socket.create_connection(("127.0.0.1", PROXY_PORT), timeout=30)
    s.sendall(b"POST /api/plan/v3/chat/completions HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n"
              b'Content-Length: 52\r\n\r\n{"model":"m","messages":[],"stream":true,"pad":"xx"}')
    s.recv(4096)
    s.close()
    time.sleep(0.5)
    st, d, _ = post("/api/plan/v3/chat/completions", b'{"model":"m","messages":[]}')
    print(f"[T8] client abort mid-stream: proxy still alive, follow-up request {st}")

    proxy.terminate()
    upstream.shutdown()
    print("done")


if __name__ == "__main__":
    main()
