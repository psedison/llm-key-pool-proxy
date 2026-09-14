"""本地假火山上游：按 Key 尾号轮转返回 403 -> 429(配额) -> 200，用于自测代理换 Key 逻辑。

仅测试用。启动：python mock_upstream.py  （端口 9901）
"""
import itertools
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = 9901
_lock = threading.Lock()
_counters: dict[str, int] = {}


class MockHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _key_tail(self) -> str:
        auth = self.headers.get("Authorization", "")
        return auth[-4:] if len(auth) >= 4 else "****"

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(length) or b"{}") if length else {}
        tail = self._key_tail()
        with _lock:
            n = _counters.get(tail, 0)
            _counters[tail] = n + 1

        wants_stream = bool(body.get("stream"))

        # 每个 Key 的第 1 次请求 403，第 2 次 429 配额，之后成功
        if n == 0:
            return self._respond(403, {"error": {"code": "AccessDenied", "message": f"key {tail} forbidden (mock)"}})
        if n == 1:
            return self._respond(429, {"error": {"code": "PlanQuotaExceeded", "message": f"key {tail} plan quota exceeded (mock)"}})

        if wants_stream:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Connection", "close")  # 无长度头，读到 EOF 结束
            self.end_headers()
            for i in range(3):
                chunk = {
                    "id": "mock-1",
                    "choices": [{"delta": {"content": f"[{tail}-chunk{i}] "}}],
                }
                self.wfile.write(f"data: {json.dumps(chunk)}\n\n".encode())
                self.wfile.flush()
            # OpenAI 兼容：最后一个 chunk 带 usage
            final = {
                "id": "mock-1",
                "choices": [{"delta": {}}],
                "usage": {"prompt_tokens": 5, "completion_tokens": 7, "total_tokens": 12},
            }
            self.wfile.write(f"data: {json.dumps(final)}\n\n".encode())
            self.wfile.flush()
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
            return
        return self._respond(200, {
            "id": "mock-1", "object": "chat.completion",
            "choices": [{"message": {"role": "assistant", "content": f"hello from key {tail} (attempt {n})"}}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3},
            "_served_by": tail,
        })

    def _respond(self, status: int, payload: dict) -> None:
        data = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    print(f"mock upstream on http://127.0.0.1:{PORT}")
    ThreadingHTTPServer(("127.0.0.1", PORT), MockHandler).serve_forever()
