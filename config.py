"""集中配置：环境变量优先，其次项目目录下的 .env 文件，兜底内置默认值。"""
import os


def _load_dotenv() -> None:
    """读取项目目录下的 .env 作为环境变量默认值；已存在的真实环境变量优先。"""
    path = os.path.join(os.path.dirname(__file__), ".env")
    if not os.path.isfile(path):
        return
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


_load_dotenv()

PROXY_HOST = os.environ.get("PROXY_HOST", "0.0.0.0")
PROXY_PORT = int(os.environ.get("PROXY_PORT", "8787"))

# 兼容保留：极少数部署想给所有 Key 统一兜底地址时可设此环境变量（不推荐，Key 应各自显式绑定）。
FALLBACK_BASE_URL = os.environ.get("FALLBACK_BASE_URL", "")

# 允许转发到上游的路径前缀（相对路径；代理按最长前缀匹配）
UPSTREAM_PATH_PREFIXES = tuple(
    p.strip()
    for p in os.environ.get(
        "UPSTREAM_PATH_PREFIXES", "/api/plan/v3,/api/v3,/v3,/v1"
    ).split(",")
    if p.strip()
)

# Key 池来源：环境变量 KEYPOOL_KEYS（逗号分隔）优先，其次 keys.txt（每行一个）
KEYS_FILE = os.environ.get("KEYS_FILE", os.path.join(os.path.dirname(__file__), "keys.txt"))

# priority：按 keys.txt 顺序固定使用第一个可用 Key（缓存最友好，失败才顺延）
# round_robin：轮询；random：随机
# rotation：窗口轮换（共享池用）——所有流量打在活跃 Key 上，三触发器任一先到即切下一把
KEY_PICK_STRATEGY = os.environ.get("KEY_PICK_STRATEGY", "priority")
# rotation 窗口触发器：0=关闭；三个全 0 时 rotation 策略启动报错
ROTATION_WINDOW_TOKENS = int(os.environ.get("ROTATION_WINDOW_TOKENS", "0"))
ROTATION_WINDOW_REQUESTS = int(os.environ.get("ROTATION_WINDOW_REQUESTS", "0"))
ROTATION_WINDOW_SECONDS = float(os.environ.get("ROTATION_WINDOW_SECONDS", "0"))
KEY_COOLDOWN_SECONDS = float(os.environ.get("KEY_COOLDOWN_SECONDS", "60"))
KEY_QUOTA_COOLDOWN_SECONDS = float(os.environ.get("KEY_QUOTA_COOLDOWN_SECONDS", "120"))
KEY_RATELIMIT_COOLDOWN_SECONDS = float(os.environ.get("KEY_RATELIMIT_COOLDOWN_SECONDS", "5"))
# 失败冷却按指数退避：base * 2^(连续失败-1)，封顶 KEY_COOLDOWN_MAX_SECONDS
KEY_COOLDOWN_MAX_SECONDS = float(os.environ.get("KEY_COOLDOWN_MAX_SECONDS", "3600"))
KEY_MAX_CONSECUTIVE_FAILS = int(os.environ.get("KEY_MAX_CONSECUTIVE_FAILS", "3"))

UPSTREAM_TIMEOUT_SECONDS = float(os.environ.get("UPSTREAM_TIMEOUT_SECONDS", "600"))
# 读流式响应时，单块之间最长等待时间
STREAM_READ_TIMEOUT_SECONDS = float(os.environ.get("STREAM_READ_TIMEOUT_SECONDS", "300"))

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
# 设置后日志同时写入该文件（按天轮转，保留 14 天）；控制台输出不受影响
LOG_FILE = os.environ.get("LOG_FILE", "")
