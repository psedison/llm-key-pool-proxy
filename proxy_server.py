"""火山 Agent Plan Key 池代理服务。

- 下游请求 → 按 池策略 取一个可用 Key → 转发到火山官方接口 → 回传响应（支持 SSE 流式）
- 401/403/429/配额不足/5xx/网络错误 → 标记失败并自动换下一个 Key 重试
- GET /pool/status、POST /pool/recover 管理接口

仅用 Python 标准库（http.server + urllib），无第三方依赖。
"""
import atexit
import faulthandler
import http.client
import json
import logging
import os
import re
import signal
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import config
from key_pool import DEFAULT_GROUP, KeyEntry, KeyPool, mask_key

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL.upper(), logging.INFO),
    format=LOG_FORMAT,
)
if config.LOG_FILE:
    # 日志落盘（按天轮转）：控制台关闭/进程被杀后仍有完整现场，控制台输出不变
    from logging.handlers import TimedRotatingFileHandler
    _log_dir = os.path.dirname(config.LOG_FILE)
    if _log_dir:
        os.makedirs(_log_dir, exist_ok=True)
    _fh = TimedRotatingFileHandler(config.LOG_FILE, when="midnight",
                                   backupCount=14, encoding="utf-8")
    _fh.setFormatter(logging.Formatter(LOG_FORMAT))
    logging.getLogger().addHandler(_fh)
log = logging.getLogger("proxy")

# 上游响应里出现这些片段视为“配额/账号不可用”，与 403/429 同等对待
QUOTA_ERROR_MARKERS = (
    "quota",
    "arrearage",
    "insufficient",
    "balance",
    "AccessDenied",
    "expired",
    "PlanQuota",
    "FreeQuota",
    "欠费",
    "配额",
    "额度",
)

# 火山真实报文样例（2026-09）：
# ① 配额打满（长冷却）：
# {"code":"AccountQuotaExceeded","message":"You have exceeded the 5-hour usage quota.
#  It will reset at 2026-09-13 22:36:33 +0800 CST. ...","type":"TooManyRequests"}
# ② 请求过于频繁（短冷却）：
# {"code":"AccountRateLimitExceeded","message":"Requests are too frequent. Please reduce
#  your request frequency, wait a short moment, and retry your request. ...","type":"TooManyRequests"}
# ①从 message 中提取重置时间，冷却直接排到该时刻；②只需短暂冷却几秒即可重试。
RESET_TIME_PATTERNS = (
    re.compile(r"reset at (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) ([+-]\d{4})"),
)

# 频率限流（区别于配额打满）：短冷却，恢复快
RATE_LIMIT_MARKERS = (
    "ratelimit",
    "rate limit",
    "too frequent",
    "requests are too",
    "请求过于频繁",
    "频率",
)

# 模型/服务不匹配（该 Key 的上游不提供此请求所需的能力）：换 Key 重试，不冷却
# 真实报文样例：{"code":"UnsupportedModel","message":"The requested model does not
# support the agent plan feature. ..."}
MODEL_MISMATCH_MARKERS = (
    "unsupportedmodel",
    "model_not_found",       # 火山/new_api 系下划线格式（真实报文 2026-09-23）
    "model not found",
    "no available channel",  # new_api 分发器：该分组下无此模型渠道
    "does not support",
    "not support the",
    "不支持",
)

# 这些路径不转发上游，属代理自身管理接口
MANAGEMENT_PATHS = ("/pool/status", "/pool/recover")


# ---------- Key 池装载 ----------

def resolve_group_and_path(group_names: set, path: str,
                           query_group: str | None = None) -> tuple[str, str]:
    """解析请求归属的 Key 分组与转发路径。

    规则：
    - 查询参数 __pool=<组名>（query_group 或路径自带）显式指定组，转发路径原样保留
    - 路径首段命中已注册组名 → 该组；剥离 "/组名" 段，剩余为转发路径
    - 其余情况（含根路径 / 与未知首段）→ default 组，路径原样
    """
    raw = urllib.parse.urlsplit(path)
    if not query_group:
        query_group = urllib.parse.parse_qs(raw.query).get("__pool", [None])[0]
    if query_group:
        return query_group.strip() or DEFAULT_GROUP, raw.path or "/"
    seg0 = raw.path.lstrip("/").split("/", 1)[0]
    if seg0 and seg0 in group_names:
        # 路径形如 "/<组名>/..."：组名前有一个 '/'，剥离时要把两个字符都算上
        rest = raw.path[1 + len(seg0):]
        return seg0, "/" + rest.lstrip("/")
    return DEFAULT_GROUP, raw.path or "/"


def parse_key_line(line: str) -> tuple[str, str, frozenset] | None:
    """解析一行 Key 配置："key|base_url" 或 "key|base_url|分组1,分组2"。

    返回 (key, base_url, groups)；未写分组时归入 default 组。
    """
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    parts = line.split("|")
    key = parts[0].strip()
    base_url = parts[1].strip().rstrip("/") if len(parts) > 1 else ""
    groups_raw = "|".join(parts[2:]) if len(parts) > 2 else ""  # 分组段内的竖线原样保留
    groups = {g.strip() for g in groups_raw.split(",") if g.strip()} if groups_raw else set()
    if not groups:
        groups = {DEFAULT_GROUP}
    return key, base_url, frozenset(groups)


def load_entries() -> list[KeyEntry]:
    """Key 池条目来源：KEYPOOL_KEYS 环境变量优先，其次 keys.txt。

    每项格式 "key|base_url" 或 "key|base_url|分组1,分组2"，base_url 必填
    （无默认上游假设）。分组未写时归 default 组。
    Key 字符串重复的行只保留第一次出现——重复几乎总是配置笔误，会打
    WARNING 指明行号；同 Key 不同地址也会被去重并特别提示。
    """
    raw: list[str]
    source: str
    env_keys = os.environ.get("KEYPOOL_KEYS", "")
    if env_keys.strip():
        raw = env_keys.split(",")
        source = "KEYPOOL_KEYS env var"
    elif os.path.isfile(config.KEYS_FILE):
        with open(config.KEYS_FILE, "r", encoding="utf-8") as f:
            raw = f.readlines()
        source = config.KEYS_FILE
    else:
        return []
    first_seen: dict[str, int] = {}
    entries: list[KeyEntry] = []
    for lineno, item in enumerate(raw, 1):
        parsed = parse_key_line(item)
        if not parsed:
            continue
        key, base_url, groups = parsed
        if key in first_seen:
            extra = " (注意：同 Key 但 base_url 不同)" if base_url != entries[
                next(i for i, e in enumerate(entries) if e.key == key)].base_url else ""
            log.warning(
                "duplicate key %s at %s line %d ignored; first seen at line %d%s",
                mask_key(key), source, lineno, first_seen[key], extra,
            )
            continue
        first_seen[key] = lineno
        if base_url and not urllib.parse.urlsplit(base_url).path.rstrip("/"):
            log.warning(
                "key %s binds a pathless base_url (%s): the downstream path will be "
                "mirrored onto it verbatim. If the upstream serves its API under a "
                "path (e.g. /v1), include it: %s/<path>",
                mask_key(key), base_url, base_url)
        entries.append(KeyEntry(key=key, base_url=base_url, groups=set(groups)))
    log.info("key source: %s | %d lines -> %d keys", source, len(raw), len(entries))
    return entries


# ---------- 上游错误判定 ----------

def is_retryable_status(status: int) -> bool:
    # 401/403 鉴权失败；404 服务/模型不匹配（换 Key 换地址重试）；
    # 408/425/429 限流；5xx 上游故障 → 换 Key
    return status in (401, 403, 404, 408, 425, 429) or status >= 500


def looks_like_quota_error(body: bytes) -> bool:
    if not body:
        return False
    text = body[:4096].decode("utf-8", errors="replace").lower()
    return any(m.lower() in text for m in QUOTA_ERROR_MARKERS)


def parse_reset_time(body: bytes) -> float | None:
    """从配额错误报文中解析"reset at YYYY-MM-DD HH:MM:SS +0800"，返回 epoch 秒。"""
    if not body:
        return None
    text = body[:4096].decode("utf-8", errors="replace")
    for pat in RESET_TIME_PATTERNS:
        m = pat.search(text)
        if not m:
            continue
        try:
            import calendar
            naive = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
            sign = 1 if m.group(2)[0] == "+" else -1
            hh, mm = int(m.group(2)[1:3]), int(m.group(2)[3:5])
            offset_sec = sign * (hh * 3600 + mm * 60)
            # naive 时间按报文声明的时区换算成 epoch
            return calendar.timegm(naive.timetuple()) - offset_sec
        except (ValueError, OverflowError):
            continue
    return None


def extract_json_usage(body: bytes) -> dict | None:
    """从非流式 JSON 响应中提取 usage；解析失败返回 None（不影响转发）。"""
    try:
        usage = json.loads(body).get("usage")
        return usage if isinstance(usage, dict) and usage else None
    except (ValueError, UnicodeDecodeError):
        return None


def build_upstream_url(base_url: str, downstream_path: str) -> str:
    """上游 URL = Key 绑定地址 + 端点后缀。

    对外暴露的路径前缀只是代理自己的命名空间：剥离已知前缀后，把剩余的
    端点后缀拼到 Key 自己的地址后面。Key 的地址（含其路径部分）完全决定
    上游服务，与对外前缀零耦合——客户端想配什么前缀都行。
    未设路径的纯域名 Key 保持旧透明语义：完整镜像下游路径。
    """
    d = urllib.parse.urlsplit(downstream_path)
    b = urllib.parse.urlsplit(base_url)
    bpath = b.path.rstrip("/")
    if not bpath:
        upstream_path = d.path  # 纯域名 Key：透明镜像
    else:
        suffix = d.path
        for p in sorted(config.UPSTREAM_PATH_PREFIXES, key=len, reverse=True):
            if d.path == p or d.path.startswith(p + "/"):
                suffix = d.path[len(p):] or "/"
                break
        if not suffix.startswith("/"):
            suffix = "/" + suffix
        upstream_path = bpath + suffix
    return urllib.parse.urlunsplit((b.scheme, b.netloc, upstream_path, d.query, ""))


def classify_failure(status: int, body: bytes) -> str:
    """返回失败类别：quota | ratelimit | mismatch | auth | server。决定冷却时长。

    顺序：先按报文精确归类（ratelimit → quota → 模型不匹配），再按状态码兜底。
    429/408/425 的语义就是"限流/重试"，无论报文文案是什么，都不得落为
    server——server 类会计入禁用阈值且冷却更长，历史上曾因火山未知的
    429 文案把整个 Key 池打瘫（真实事故 2026-09-15）。
    404 归为 mismatch（模型/服务不匹配，如 plan 请求打到 coding Key 上）：
    不是 Key 的错，换下一把 Key 用它自己的地址重试。
    """
    if looks_like_rate_limit_error(body):
        return "ratelimit"
    if looks_like_quota_error(body):
        return "quota"
    if looks_like_model_mismatch(body):
        return "mismatch"
    if status in (401, 403):
        return "auth"
    if status in (408, 425, 429):
        return "ratelimit"  # 报文未识别的限流类状态码：按频率限流兜底（短冷却、不禁用）
    if status == 404:
        return "mismatch"  # 未知 404：大概率该 Key 的服务不提供此端点/模型
    return "server"


def looks_like_model_mismatch(body: bytes) -> bool:
    if not body:
        return False
    text = body[:4096].decode("utf-8", errors="replace").lower()
    return any(m in text for m in MODEL_MISMATCH_MARKERS)


def looks_like_rate_limit_error(body: bytes) -> bool:
    if not body:
        return False
    text = body[:4096].decode("utf-8", errors="replace").lower()
    return any(m in text for m in RATE_LIMIT_MARKERS)


# ---------- 代理 Handler ----------

class ProxyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "llm-key-pool-proxy/1.0"
    pool: KeyPool  # 由 run() 注入

    def handle_one_request(self):
        """下游 keep-alive 上突然断开（10054）时 socketserver 会打整页 traceback；
        断连是常态，静默处理。"""
        try:
            super().handle_one_request()
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, TimeoutError):
            self.close_connection = True

    # ----- 基础工具 -----

    def _send_json(self, status: int, payload: dict) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _read_request_body(self) -> bytes:
        # 优先按 Content-Length 读；无长度头（chunked 请求）时读到 EOF
        length_header = self.headers.get("Content-Length")
        if length_header:
            try:
                length = int(length_header)
            except ValueError:
                length = 0
            if length > 0:
                return self.rfile.read(length)
            return b""
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            # http.server 不自动解码 chunked 请求体，手工按 RFC7230 解析
            chunks = []
            while True:
                size_line = self.rfile.readline(1024).strip()
                if b";" in size_line:
                    size_line = size_line.split(b";", 1)[0]
                try:
                    size = int(size_line, 16)
                except ValueError:
                    break
                if size == 0:
                    self.rfile.readline(1024)  # 尾部 CRLF
                    break
                chunks.append(self.rfile.read(size))
                self.rfile.readline(1024)  # 每块后的 CRLF
            return b"".join(chunks)
        return b""

    def _is_management(self) -> bool:
        return self.path.split("?")[0] in MANAGEMENT_PATHS

    # ----- 管理接口 -----

    def _handle_management(self, method: str) -> None:
        path = self.path.split("?")[0]
        if path == "/pool/status" and method == "GET":
            self._send_json(200, self.pool.status())
            return
        if path == "/pool/recover" and method == "POST":
            body = self._read_request_body()
            key_tail = None
            if body:
                try:
                    key_tail = json.loads(body.decode("utf-8")).get("key_tail")
                except (ValueError, AttributeError):
                    pass
            recovered = self.pool.recover_all(key_tail)
            self._send_json(200, {"recovered": recovered, **self.pool.status()})
            return
        self._send_json(405, {"error": "method not allowed for management endpoint"})

    # ----- 上游转发 -----

    def _build_upstream_request(self, body: bytes, entry, fwd_path: str) -> urllib.request.Request:
        # 上游 URL 完全由 Key 自己的地址决定：剥离对外前缀与组名段（代理的命名空间），
        # 端点后缀拼到 Key 地址后面——对外路径与后端地址零耦合
        upstream_url = build_upstream_url(entry.base_url, fwd_path)
        headers = {
            "Content-Type": self.headers.get("Content-Type", "application/json"),
            "Accept": self.headers.get("Accept", "application/json"),
            "Authorization": f"Bearer {entry.key}",
        }
        # 透传部分对上游有意义的头（丢弃下游鉴权与逐跳头）
        for name in ("X-Request-Id", "User-Agent"):
            if self.headers.get(name):
                headers[name] = self.headers[name]
        return urllib.request.Request(
            upstream_url, data=body if body else None,
            headers=headers, method=self.command,
        )

    def _forward_once(self, body: bytes, entry, fwd_path: str):
        """用 entry.key 请求上游一次。

        返回 (kind, resp_or_error)：
        - ("ok", HTTPResponse)：成功，调用方负责回传
        - ("retryable", (status, body_bytes, kind))：可重试失败
        - ("fatal", (status, body_bytes))：不可重试失败（其他 4xx）
        - ("network", (status, message))：连不上上游（status 502/504 语义）
        """
        req = self._build_upstream_request(body, entry, fwd_path)
        # urllib 的 timeout 是 socket 级单值（作用于连接与每次读），不支持元组
        timeout = (
            config.STREAM_READ_TIMEOUT_SECONDS
            if self._client_wants_stream(body)
            else config.UPSTREAM_TIMEOUT_SECONDS
        )
        try:
            resp = urllib.request.urlopen(req, timeout=timeout)
        except urllib.error.HTTPError as e:
            err_body = e.read() or b""
            if is_retryable_status(e.code):
                kind = classify_failure(e.code, err_body)
                reset_at = parse_reset_time(err_body) if kind == "quota" else None
                return "retryable", (e.code, err_body, kind, reset_at)
            return "fatal", (e.code, err_body)
        except (urllib.error.URLError, OSError, TimeoutError) as e:
            # 网络故障（DNS/连接/超时）不是任何 Key 的错：不记账、不冷却、不禁用。
            # 换 Key 重试仍有意义——多个上游地址时可能恰好某个可达。
            return "network", (502, str(e).encode("utf-8"))
        # 2xx 但响应体带配额错误标记的兜底（个别网关 200 包错误 JSON 的情况少见，仅提示）
        return "ok", resp

    @staticmethod
    def _client_wants_stream(body: bytes) -> bool:
        if not body:
            return False
        try:
            return bool(json.loads(body.decode("utf-8")).get("stream"))
        except (ValueError, UnicodeDecodeError):
            return False

    # ----- 主转发循环（含换 Key 重试） -----

    def _resolve_group_and_path(self) -> tuple[str, str]:
        return resolve_group_and_path(self.pool.group_names(), self.path)

    def _send_group_info(self, group: str) -> None:
        self._send_json(200, self.pool.group_info(group))

    def _relay(self) -> None:
        body = self._read_request_body()
        wants_stream = self._client_wants_stream(body)
        group, fwd_path = self._resolve_group_and_path()

        # 裸组路径或根路径：返回组概要（核对分组配置用），不打上游
        if fwd_path in ("", "/"):
            return self._send_group_info(group)

        # 组名之后的剩余路径 = 端点后缀，原样拼到"选中 Key 自己的地址"后面。
        # 已知暴露前缀（UPSTREAM_PATH_PREFIXES）若出现会被剥离（兼容镜像式
        # Base URL）；拼错路径由上游 404，无需代理白名单。
        max_attempts = max(1, len(self.pool.keys))
        last_err_status, last_err_body = 503, b'{"error":"no usable key"}'
        tried: set[str] = set()  # 同一请求内已尝试的 Key：网络失败不冷却，但不能反复撞同一把

        for attempt in range(1, max_attempts + 1):
            entry, usable = self.pool.acquire(exclude=tried, group=group)
            if entry is None:
                log.warning("no usable key in group '%s' at attempt %d (%d already tried)",
                            group, attempt, len(tried))
                break
            tried.add(entry.key)
            log.info(
                "try %d of %d using key %s (group=%s, usable=%d) %s -> %s%s [upstream=%s]",
                attempt, max_attempts, mask_key(entry.key), group, usable, self.command,
                fwd_path, " [stream]" if wants_stream else "",
                entry.base_url,
            )
            kind, result = self._forward_once(body, entry, fwd_path)

            if kind == "ok":
                resp = result
                # 读一小段探测：个别上游 200 + 错误 JSON 的兜底判定
                first_chunk = resp.read1(65536) if hasattr(resp, "read1") else b""
                if first_chunk and not wants_stream and looks_like_quota_error(first_chunk):
                    reset_at = parse_reset_time(first_chunk)
                    self.pool.report_failure(entry, "200-with-quota-error", "quota",
                                             cooldown_until=reset_at)
                    rest = first_chunk + resp.read()
                    last_err_status, last_err_body = 403, rest
                    continue
                self.pool.report_success(entry)
                self._relay_response(resp, first_chunk, wants_stream, group)
                return

            # 失败分支
            reset_at = None
            if kind == "retryable":
                status, err_body, fail_kind, reset_at = result
            elif kind == "network":
                # 网络故障：不记 Key 失败、不冷却、不禁用（Key 本身没问题）。
                # 直接换下一个 Key 重试；全部 Key 都连不上则快速失败。
                log.warning(
                    "network error reaching upstream for key %s (%s); trying next key",
                    mask_key(entry.key), result[1].decode("utf-8", "replace")[:120],
                )
                last_err_status, last_err_body = result[0], result[1]
                continue
            else:  # fatal：其他 4xx 不是 Key 的问题，直接回传
                status, err_body = result
                self._relay_error(status, err_body)
                return

            disabled = self.pool.report_failure(
                entry, f"HTTP {status}: {err_body[:200]!r}", fail_kind,
                cooldown_until=reset_at,
            )
            if fail_kind == "mismatch":
                detail = "rotate (no cooldown: key or its upstream cannot serve this request)"
            elif fail_kind == "quota" and reset_at:
                until = datetime.fromtimestamp(reset_at).strftime("%Y-%m-%d %H:%M:%S")
                detail = f"cooldown until quota reset {until}"
            else:
                detail = "DISABLED" if disabled else "cooldown"
            log.warning(
                "key %s failed (%s, HTTP %d), %s; switch to next key; body=%r",
                mask_key(entry.key), fail_kind, status, detail,
                err_body[:300].decode("utf-8", "replace"),
            )
            last_err_status, last_err_body = status, err_body

        # 所有尝试用尽 / 无可用 Key
        self._relay_pool_exhausted(last_err_status, last_err_body, group)

    def _relay_pool_exhausted(self, status: int, err_body: bytes, group: str = DEFAULT_GROUP) -> None:
        st = self.pool.status()
        log.error("all attempts exhausted (last upstream status=%d, usable=%d/%d)",
                  status, st["usable"], st["total"])
        payload = {
            "error": {
                "message": f"All keys in group '{group}' failed or pool exhausted (last upstream error attached)",
                "type": "key_pool_exhausted",
                "group": group,
                "last_upstream_status": status,
                "pool": st,
            }
        }
        if err_body:
            try:
                payload["error"]["last_upstream_error"] = json.loads(err_body.decode("utf-8", "replace"))
            except ValueError:
                payload["error"]["last_upstream_error"] = err_body.decode("utf-8", "replace")[:1000]
        self._send_json(503, payload)

    def _relay_error(self, status: int, body: bytes) -> None:
        """非 Key 原因的 4xx：原样透传上游错误。"""
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    @staticmethod
    def _log_usage(usage: dict) -> None:
        """请求完成后输出 token 用量：in/out/缓存命中率/推理 token。"""
        cached = usage.get("prompt_tokens_details", {}).get("cached_tokens") if isinstance(
            usage.get("prompt_tokens_details"), dict) else None
        reasoning = usage.get("completion_tokens_details", {}).get("reasoning_tokens") if isinstance(
            usage.get("completion_tokens_details"), dict) else None
        parts = [
            f"in={usage.get('prompt_tokens', '?')}",
            f"out={usage.get('completion_tokens', '?')}",
        ]
        if cached is None:
            # 上游未报缓存字段（第三方网关常见）：显式 n/a，与"报了但为 0"区分开
            parts.append("cached=n/a")
        else:
            prompt = usage.get("prompt_tokens") or 0
            hit_pct = f" ({cached * 100 // prompt}%)" if prompt else ""
            parts.append(f"cached={cached}{hit_pct}")
        if reasoning:
            parts.append(f"reasoning={reasoning}")
        log.info("usage: %s", " ".join(parts))

    def _record_usage(self, group: str, usage: dict) -> None:
        """输出 token 用量日志，并把消耗计入所属分组的 rotation 窗口计数。"""
        self._log_usage(usage)
        tokens = int(usage.get("prompt_tokens") or 0) + int(usage.get("completion_tokens") or 0)
        if tokens > 0:
            self.pool.report_usage(group, tokens)

    def _relay_response(self, resp, first_chunk: bytes, wants_stream: bool, group: str) -> None:
        """把上游响应回传下游；流式逐块转发，非流式也按块回写（支持任意大小）。"""
        status = resp.status
        headers = resp.headers
        self.send_response(status)
        is_sse = wants_stream or (headers.get_content_type() == "text/event-stream")
        if is_sse:
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "close")
        else:
            self.send_header("Content-Type", headers.get_content_type() or "application/json")
        self.close_connection = True  # 统一按 EOF 语义收尾，避免大响应 Content-Length 失配
        self.end_headers()

        def pump(reader) -> int:
            sent = 0
            # 首块写入同样要防下游断连（12:16 的真实 traceback 就出在这里：
            # 客户端在首块到达前断开，异常穿透到 _dispatch 打出整页堆栈）
            if first_chunk:
                try:
                    self.wfile.write(first_chunk)
                except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError) as e:
                    log.info("client disconnected before first byte: %s", e)
                    self.close_connection = True
                    try:
                        resp.close()
                    except OSError:
                        pass
                    return 0
                sent += len(first_chunk)
            while True:
                try:
                    chunk = reader()
                except (TimeoutError, OSError, http.client.IncompleteRead,
                        http.client.RemoteDisconnected) as e:
                    # 上游 chunked 流中途截断（IncompleteRead）/提前断开：
                    # 已发给下游的部分无法撤回，无法换 Key 重试，只能断流收尾。
                    # 这是上游侧传输问题，不是代理缺陷——单行 ERROR，不刷 traceback。
                    log.error(
                        "upstream stream truncated after %d bytes sent to client (%s); "
                        "client sees an incomplete response", sent, e,
                    )
                    self.close_connection = True
                    break
                if not chunk:
                    break
                try:
                    self.wfile.write(chunk)
                except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError) as e:
                    # 下游主动断开（用户停止生成/客户端超时/DSH 切会话）：
                    # 属正常现象。必须停掉上游读取并关闭，避免 traceback 刷屏和上游连接悬挂。
                    log.info("client disconnected mid-stream after %d bytes: %s", sent, e)
                    self.close_connection = True
                    try:
                        resp.close()
                    except OSError:
                        pass
                    return sent
                sent += len(chunk)
                if is_sse:
                    self.wfile.flush()
            return sent

        # usage 提取：非流式解析完整 JSON；流式扫描 SSE 块取最后一个 usage
        # （OpenAI 兼容流的最后一个 chunk 常带 usage 字段；解析失败静默跳过，不影响转发）
        # 注意：first_chunk（探测块）可能已吃掉小响应的全部字节，必须参与提取。
        def extract_usage(chunk: bytes) -> dict | None:
            if is_sse:
                for line in chunk.split(b"\n"):
                    line = line.strip()
                    if not line.startswith(b"data:"):
                        continue
                    payload = line[5:].strip()
                    if not payload or payload == b"[DONE]":
                        continue
                    try:
                        data = json.loads(payload)
                        usage = data.get("usage")
                        if isinstance(usage, dict) and usage:
                            return usage
                    except ValueError:
                        continue
                return None
            # 非流式：JSON 可能跨块，缓存累积后从尾部解析
            json_buf.append(chunk)
            del json_buf[:-4]  # 最多保留尾部 4 块（≈256KB，足够覆盖 usage）
            try:
                usage = json.loads(b"".join(json_buf)).get("usage")
                return usage if isinstance(usage, dict) and usage else None
            except (ValueError, UnicodeDecodeError):
                return None

        json_buf: list = []
        if is_sse:
            usage_holder: list = []

            def reader_sse():
                c = resp.read1(65536)
                if c:
                    u = extract_usage(c)
                    if u:
                        usage_holder.clear()
                        usage_holder.append(u)
                return c

            sent = pump(reader_sse)
            # first_chunk（探测块）的 usage 优先级最低；流式最后一个 usage chunk 才权威
            if not usage_holder and first_chunk:
                u = extract_usage(first_chunk)
                if u:
                    usage_holder.append(u)
            if usage_holder:
                self._record_usage(group, usage_holder[0])
            return

        # 非流式：块式转发（65536B/块），总长度自动匹配，无 IncompleteRead 风险
        def reader_json():
            return resp.read(65536)

        sent = pump(reader_json)
        # 小响应被 first_chunk 整包吃掉时 json_buf 为空，用 first_chunk 兜底解析
        if json_buf:
            u = extract_usage(b"")
        else:
            u = extract_json_usage(first_chunk) if first_chunk else None
        if u:
            self._record_usage(group, u)
        log.debug("relayed %d bytes non-stream", sent)

    # ----- HTTP 方法入口 -----

    def _dispatch(self) -> None:
        try:
            if self._is_management():
                self._handle_management(self.command)
                return
            self._relay()
        except (BrokenPipeError, ConnectionResetError):
            log.info("client disconnected: %s", self.path)
        except Exception:
            log.exception("internal error handling %s %s", self.command, self.path)
            try:
                self._send_json(500, {"error": "internal proxy error"})
            except OSError:
                pass

    def do_GET(self):  # noqa: N802
        self._dispatch()

    def do_POST(self):  # noqa: N802
        self._dispatch()

    def do_DELETE(self):  # noqa: N802
        self._dispatch()

    def do_PATCH(self):  # noqa: N802
        self._dispatch()

    def do_PUT(self):  # noqa: N802
        self._dispatch()

    def log_message(self, fmt: str, *args) -> None:  # 静默默认访问日志，用结构化日志代替
        pass


# ---------- 启动 ----------

def run() -> None:
    entries = load_entries()
    if not entries:
        log.error(
            "no keys configured. Put keys into %s (one per line as 'key|base_url')"
            " or set KEYPOOL_KEYS env var (comma-separated 'key|base_url').",
            config.KEYS_FILE,
        )
        sys.exit(1)
    # 每个 Key 必须显式绑定上游地址：代理是通用转发器，不做任何默认上游假设
    unbound = [mask_key(e.key) for e in entries if not e.base_url]
    if unbound:
        log.error(
            "keys without base_url: %s. Every key must be configured as 'key|base_url'"
            " (e.g. 'ag-xxx|https://ark.cn-beijing.volces.com/api/plan/v3')."
            " No default upstream is assumed.",
            ", ".join(unbound),
        )
        sys.exit(1)
    if config.KEY_PICK_STRATEGY == "rotation" \
            and max(config.ROTATION_WINDOW_TOKENS, config.ROTATION_WINDOW_REQUESTS) <= 0 \
            and config.ROTATION_WINDOW_SECONDS <= 0:
        log.error(
            "strategy=rotation requires at least one window trigger > 0: "
            "ROTATION_WINDOW_TOKENS / ROTATION_WINDOW_REQUESTS / ROTATION_WINDOW_SECONDS"
        )
        sys.exit(1)
    pool = KeyPool.from_entries(
        entries,
        strategy=config.KEY_PICK_STRATEGY,
        cooldown_seconds=config.KEY_COOLDOWN_SECONDS,
        quota_cooldown_seconds=config.KEY_QUOTA_COOLDOWN_SECONDS,
        ratelimit_cooldown_seconds=config.KEY_RATELIMIT_COOLDOWN_SECONDS,
        cooldown_max_seconds=config.KEY_COOLDOWN_MAX_SECONDS,
        max_consecutive_fails=config.KEY_MAX_CONSECUTIVE_FAILS,
        window_tokens=config.ROTATION_WINDOW_TOKENS,
        window_requests=config.ROTATION_WINDOW_REQUESTS,
        window_seconds=config.ROTATION_WINDOW_SECONDS,
    )
    log.info("loaded %d keys (strategy=%s):", len(entries), pool.strategy)
    for e in entries:
        log.info("  %s -> %s", mask_key(e.key), e.base_url)

    # 分组名冲突预警：组名通过 URL 路径首段选择，与保留字/暴露前缀重名会被遮蔽
    prefix_first_segs = {p.strip("/").split("/")[0] for p in config.UPSTREAM_PATH_PREFIXES}
    for g in sorted(pool.group_names()):
        if g in ("pool",):
            log.warning("group '%s' is reserved (/pool/* = management paths); "
                        "URL requests cannot reach it - rename the group", g)
        elif g.split("/", 1)[0] in prefix_first_segs:
            log.warning("group '%s' shares its first segment with an exposed path "
                        "prefix - group selection takes precedence, mirror-form "
                        "default routing for that segment is shadowed", g)

    _STRATEGY_DESC = {
        "priority": "粘住第一把可用 Key（缓存最友好，适合独享池）",
        "rotation": "窗口轮换，额度均摊（适合多人共用池）",
        "round_robin": "逐请求轮换（每个请求换账号，缓存失效）",
        "random": "随机选取",
    }
    log.info("key strategy: %s (%s)", pool.strategy, _STRATEGY_DESC.get(pool.strategy, "unknown"))

    ProxyHandler.pool = pool

    class ProxyServer(ThreadingHTTPServer):
        daemon_threads = True  # 退出时不等待挂起的流式连接（最长 600s 的读超时会卡住关机）

        def handle_error(self, request, client_address):
            # 请求线程的未捕获异常：打进结构化日志而不是裸 stderr，
            # 避免日志断流时丢失现场
            log.error("unhandled error in request thread from %s",
                      client_address, exc_info=True)

    server = ProxyServer((config.PROXY_HOST, config.PROXY_PORT), ProxyHandler)
    faulthandler.enable()  # 段错误/abort 等致命崩溃时自动转储 Python 线程栈
    atexit.register(lambda: log.info("proxy process exiting"))
    log.info("process pid=%d", os.getpid())
    # 监听概要：0.0.0.0/:: 时额外提示本机访问地址，避免把监听接口误当访问地址
    if config.PROXY_HOST in ("0.0.0.0", "::"):
        access = f"http://127.0.0.1:{config.PROXY_PORT}"
        log.info("access URL (use this in downstream tools): %s", access)
        log.info("bind address: %s:%d (all interfaces, LAN reachable)", config.PROXY_HOST, config.PROXY_PORT)
    else:
        access = f"http://{config.PROXY_HOST}:{config.PROXY_PORT}"
        log.info("access URL (use this in downstream tools): %s", access)
    # 下游 Base URL 说明：对外前缀只是代理命名空间，任意已知前缀都可用，
    # 上游地址完全由选中的 Key 决定（与对外前缀零耦合）
    log.info(
        "downstream Base URL: %s/<prefix>/chat/completions  (prefix is arbitrary; "
        "exposed prefixes: %s; the selected key's own address decides the upstream)",
        access, ", ".join(config.UPSTREAM_PATH_PREFIXES),
    )
    log.info(
        "config: strategy=%s | cooldown base=%.0fs (quota=%.0fs, ratelimit=%.0fs, max=%.0fs)"
        " | disable threshold=%d auth fails | upstream timeout=%.0fs | keys=%d",
        pool.strategy,
        config.KEY_COOLDOWN_SECONDS,
        config.KEY_QUOTA_COOLDOWN_SECONDS,
        config.KEY_RATELIMIT_COOLDOWN_SECONDS,
        config.KEY_COOLDOWN_MAX_SECONDS,
        config.KEY_MAX_CONSECUTIVE_FAILS,
        config.UPSTREAM_TIMEOUT_SECONDS,
        len(entries),
    )
    if pool.strategy == "rotation":
        log.info(
            "rotation windows: tokens>=%d or requests>=%d or seconds>=%s (first trigger switches)",
            config.ROTATION_WINDOW_TOKENS,
            config.ROTATION_WINDOW_REQUESTS,
            config.ROTATION_WINDOW_SECONDS,
        )
        for gname in sorted(pool.group_names()):
            log.info("group '%s' active key: %s", gname, pool.rotation_status(gname)["active_key"])

    def _on_sigterm(signum, _frame):
        # docker stop / 任务管理器结束进程等会发 SIGTERM；转到独立线程优雅停机
        # （shutdown() 不允许在 serve_forever 所在线程调用，会死锁）
        log.info("received signal %s, shutting down", signum)
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, _on_sigterm)
    try:
        server.serve_forever()
        log.warning("server loop exited unexpectedly (no exception)")  # 正常情况不会走到
    except KeyboardInterrupt:
        log.info("Ctrl+C received, shutting down")
    except BaseException:
        log.exception("server loop crashed with an unexpected exception!")  # 任何崩溃都留现场
        raise
    finally:
        server.server_close()
        log.info("proxy stopped, goodbye")


if __name__ == "__main__":
    run()
