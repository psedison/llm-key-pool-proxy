# Volcano Engine Agent Plan Key-Pool Proxy (火山方舟 Key 池代理)

一个纯 Python（标准库实现，无第三方依赖）的 HTTP 代理服务：
维护一个火山引擎（Volcano Engine Ark / Agent Plan）API Key 池，收到下游请求后按**轮询或随机**策略取一个可用 Key，
把请求原样转发到火山官方接口，并把响应（含 SSE 流式）回传给下游。
遇到 **401/403（鉴权失败）或 429/配额不足** 时自动换下一个 Key 重试。

## 特性

- **Key 池管理**
  - 三种选 Key 策略：
    - `priority`（**默认**）：始终固定用排最前的可用 Key，失败才顺延到下一个——流量钉在单一账号上，**上游前缀缓存可以跨请求命中**，token 成本最低
    - `round_robin`：轮询，负载最均匀，但跨账号会打散上游缓存
    - `random`：随机
  - 可用时才参与选取；失败 Key 进入冷却并按**指数退避**（60s → 120s → 240s → … 封顶 1h；配额类从 120s 起步），冷却到点自动恢复参与选取，无需人工干预
  - 连续失败达到阈值（默认 3 次）自动禁用，可通过管理接口手动恢复
  - 线程安全（锁保护），每次选取做健康检查
- **转发**
  - 透传 method / path / query / JSON body / 流式响应（SSE 逐块转发）
  - 路径前缀可配置：默认 `/v1`、`/api/v3`（Ark 官方为 `https://ark.cn-beijing.volces.com/api/v3/...`）
  - 流式失败重试：仅当上游尚未吐出任何字节时才允许换 Key 重试，避免重复输出
- **错误处理**
  - 401 / 403 / 429 / 上游体内容含配额不足（`quota`、`Arrearage`、`AccessDenied` 等）→ 标记当前 Key 失败并换下一个 Key 重试
  - 所有 Key 都不可用时返回 `503`，并附可用/禁用明细
  - 上游 5xx 同样触发换 Key 重试（计入失败次数）
- **管理接口**
  - `GET /pool/status` — Key 池状态（每个 Key 的可用性、冷却剩余、连续失败次数；Key 只显示尾 4 位）
  - `POST /pool/recover` — 手动恢复全部禁用 Key（body 可选 `{"key_tail": "abcd"}` 精确恢复）
- **可观测性**：结构化日志输出每次选 Key、重试、冷却、禁用事件

## 文件

```
volc-keypool-proxy/
├── key_pool.py     # Key 池实现（选取、冷却、禁用、恢复、线程安全）
├── proxy_server.py # HTTP 代理服务（转发、重试、流式、管理接口）
├── config.py       # 配置（环境变量优先，兜底默认值）
├── keys.txt        # Key 池文件：每行一个 Key，# 开头为注释
├── mock_upstream.py# 本地模拟火山上游（仅用于测试，可无视）
└── README.md
```

## 快速开始

1. 配置 Key 与上游地址。**每个 Key 必须显式绑定自己的上游地址**（`key|base_url`，无默认上游假设；一个地址可挂多个 Key，一个 Key 只属一个地址）：

   编辑 `keys.txt`（每行一项，`#` 开头为注释）：

   ```
   # 格式：key|base_url（地址必填，缺了启动直接报错）
   ag-xxx01|https://ark.cn-beijing.volces.com/api/plan/v3
   ag-xxx02|https://ark.cn-beijing.volces.com/api/plan/v3
   ag-xxx03|https://ark.cn-shanghai.volces.com/api/plan/v3
   ```

   或使用环境变量（逗号分隔，同样格式，优先于 keys.txt）：

   ```bash
   KEYPOOL_KEYS="ag-xxx01|https://ark.cn-beijing.volces.com/api/plan/v3,ag-xxx02|https://ark.cn-shanghai.volces.com/api/plan/v3"
   ```

   行序即 priority 策略的优先级（第一行是主力账号，缓存钉在它上面）。

2. 启动代理：

```bash
python proxy_server.py
# 可选环境变量：
#   PROXY_HOST=0.0.0.0       监听地址
#   PROXY_PORT=8787          监听端口
#   FALLBACK_BASE_URL=        可选兜底地址（不推荐；Key 应各自显式绑定地址）
#   KEYPOOL_KEYS=key1|url1,key2,key3|url2   直接用环境变量注入 Key（优先于 keys.txt）
#   KEY_PICK_STRATEGY=priority|round_robin|random  选 Key 策略，默认 priority（固定优先级，缓存友好）
#   KEY_COOLDOWN_SECONDS=60       失败冷却基数（指数退避：60→120→240→…）
#   KEY_QUOTA_COOLDOWN_SECONDS=120 配额类失败的冷却基数
#   KEY_RATELIMIT_COOLDOWN_SECONDS=5  频率限流冷却基数
#   KEY_COOLDOWN_MAX_SECONDS=3600 冷却封顶（配额重置时间未知，靠退避探测）
#   KEY_MAX_CONSECUTIVE_FAILS=3   连续 auth 失败多少次后禁用
#   UPSTREAM_TIMEOUT_SECONDS=600  上游请求超时
#   UPSTREAM_PATH_PREFIXES=/api/plan/v3,/api/v3,/v3,/v1  对外暴露的路径前缀（命名空间，可自定义）
```

**长期运行建议用守护脚本**（进程意外退出自动重启，日志按天落盘到 `logs/`，窗口误关/崩溃后仍有据可查）：

```powershell
# Windows (PowerShell)
.\run-guarded.ps1
```

```bash
# Linux / macOS / CentOS
./run-guarded.sh                               # 前台守护
nohup ./run-guarded.sh >/dev/null 2>&1 &       # 后台守护
```

守护脚本直接**前台运行代理：控制台实时输出**，同时代理自身把日志按天落盘到 `logs/proxy-YYYYMMDD.log`（`LOG_FILE` 环境变量可改位置，按天轮转保留 14 天）。崩溃/被杀（非零退出码）3 秒后自动重启；Ctrl+C 即整体停止、不会误重启；代理被外部硬杀时，日志里 `proxy stopped` 缺失 + guard 的 `exited with code N` 行就是死因证据。

重试换 Key 时会连同该 Key 绑定的地址一起切换（请求始终发往"当前 Key 自己的地址"）。

3. 下游像调用官方接口一样调用代理：

```bash
curl http://127.0.0.1:8787/api/v3/chat/completions \
  -H "Authorization: Bearer 任意值(代理不校验)" \
  -H "Content-Type: application/json" \
  -d '{"model":"doubao-seed-1-6-250615","messages":[{"role":"user","content":"hi"}]}'
```

- 下游请求头里的 `Authorization` 会被丢弃，由代理用池中选出的 Key 重新写入 `Authorization: Bearer <key>`。
- 非流式请求返回上游 JSON；`stream: true` 时原样转发 SSE 字节流。

## 兼容 OpenAI SDK 使用

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://127.0.0.1:8787/api/v3",
    api_key="not-needed",
)
resp = client.chat.completions.create(
    model="doubao-seed-1-6-250615",
    messages=[{"role": "user", "content": "你好"}],
    stream=True,
)
for chunk in resp:
    print(chunk.choices[0].delta.content or "", end="")
```

## 重试语义

| 上游状况 | 处理 |
| --- | --- |
| 401 / 403 | Key 无效或无权限 → 冷却 + 换 Key 重试 |
| 429 配额打满（`AccountQuotaExceeded`，响应体含 quota） | 冷却 + 换 Key 重试；**永不触发禁用**，重置后 Key 自动回归 |
| 429 频率限流（`AccountRateLimitExceeded`，too frequent） | **短冷却（5s 起步退避）** + 换 Key 重试；同样永不禁用 |
| 上游报文带 `reset at YYYY-MM-DD HH:MM:SS +ZZZZ`（火山 5 小时窗口真实格式） | 冷却精确排到重置时刻，不再盲目退避 |
| 上游 5xx / 网络异常 | 换 Key 重试，计入失败 |
| 4xx（其他，如 400 参数错误） | 原样返回，不换 Key（不是 Key 的问题） |
| SSE 已输出字节后失败 | 无法安全重试，直接断流返回 |
| 所有 Key 不可用 | `503` + 池状态 JSON |

**两类 429 的区别**（来自真实报文验证）：
- `AccountQuotaExceeded`（5 小时用量窗口打满）→ 报文自带 `reset at ...`，冷却精确到该时刻
- `AccountRateLimitExceeded`（瞬时请求过频）→ "wait a short moment" 即恢复，冷却仅 5s 起步退避（`KEY_RATELIMIT_COOLDOWN_SECONDS` 可调）

**配额/限流失败何时回来？** 频率限流几秒后自动回归；配额打满按报文里的重置时刻精确回归（无重置时间的报文则退化为指数退避探测：120s→240s→…封顶 1h）。priority 策略下恢复后流量自动优先回到池顶账号。

**禁用（需人工 `/pool/recover`）是最后手段，只针对一种情况**：401/403 无效凭证连续超阈值。网络故障（DNS/连接失败/超时）**不记任何 Key 失败**——断网期间请求全部快速 503，但 Key 池零损伤，网络恢复后下一个请求立即可用；5xx 也只冷却不禁用（上游故障不证明 Key 坏）。即除"上游明确说这个 Key 无效"外，一切失败都会自动回归。

## 测试

`mock_upstream.py` 是一个本地假火山服务，用于自测（按 Key 尾号轮转返回 403 → 429 → 正常）：

```bash
python mock_upstream.py        # 端口 9901
python proxy_server.py         # KEYPOOL_KEYS=key1|http://127.0.0.1:9901
# 然后请求 http://127.0.0.1:8787/api/v3/chat/completions 观察自动换 Key
```

## 注意

- 仅做**个人账号自用**的 Key 池转发。请遵守火山引擎服务条款与配额规则。
- 代理不校验下游身份，请勿把端口暴露到公网；需要时自行在前面加网关鉴权。
