# llm-key-pool-proxy

通用 LLM API Key 池代理：维护多个上游账号的 API Key，收到下游请求后按策略选取可用 Key，
转发到**该 Key 绑定的上游地址**，并把响应（含 SSE 流式）原样回传。上游失败时自动换下一个 Key 重试。

纯 Python 标准库实现，无第三方依赖，单机即跑。

## 特性

- **每个 Key 绑定自己的上游地址**（`key|base_url`），一个地址可挂多个 Key；任何 OpenAI 兼容上游均可（火山方舟、DeepSeek、OpenAI……）
- **三种选 Key 策略**：
  - `priority`（默认）：固定用排最前的可用 Key，失败才顺延——流量钉在单一账号，**上游前缀缓存跨请求命中**，token 成本最低
  - `round_robin` / `random`
- **错误分类处理**（基于真实上游报文验证）：
  - 401/403 无效凭证 → 冷却 + 换 Key；连续 3 次禁用（唯一会禁用的情况，需 `/pool/recover` 恢复）
  - 429 配额打满（报文带 `reset at ...` 时）→ 冷却**精确到重置时刻**；不带则指数退避
  - 429 频率限流 → 短冷却（5s 起步退避），恢复快
  - 5xx / 网络故障 → 网络故障**不记 Key 失败**（断网零损伤，恢复即满血）；5xx 只冷却不禁用
- **透传保真**：请求体不解析不重构（多模态图片/视频 base64、multipart 均字节级透传）；60MB 请求体、100MB 流式响应实测通过，内存占用恒定
- **流式安全**：SSE 逐块转发；已输出后失败不重试（避免重复内容），客户端中途断开优雅收尾
- **可观测**：每次请求输出所用 Key、上游地址、token 用量（in/out/缓存命中/推理）；`/pool/status` 查看池状态

## 快速开始

1. 配置 Key（**地址必填**，无默认上游假设）：

   复制 `keys.example.txt` 为 `keys.txt`，每行一个 `key|base_url`（行序即 priority 优先级）：

   ```
   sk-xxx01|https://ark.cn-beijing.volces.com/api/plan/v3
   sk-xxx02|https://ark.cn-beijing.volces.com/api/plan/v3
   sk-xxx03|https://api.deepseek.com/v1
   ```

   或环境变量 `KEYPOOL_KEYS="sk-xxx01|https://...,sk-xxx02|https://..."`（优先于文件）。

2. 启动：

   ```bash
   python proxy_server.py
   ```

   启动日志给出每个 Key 的绑定地址、访问 URL、推荐下游 Base URL 与全部运行参数。

3. 下游像调官方接口一样调用（以 OpenAI SDK 为例）：

   ```python
   client = OpenAI(base_url="http://127.0.0.1:8787/api/plan/v3", api_key="not-needed")
   ```

   下游的 `Authorization` 会被丢弃，由代理用池中 Key 重写。

## 管理接口

- `GET /pool/status` — 每个 Key 的可用性/冷却剩余/失败计数/绑定地址
- `POST /pool/recover` — 恢复被禁用的 Key（body 可选 `{"key_tail": "abcd"}` 精确恢复）

## 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `PROXY_HOST` / `PROXY_PORT` | `0.0.0.0` / `8787` | 监听地址 |
| `KEYPOOL_KEYS` | — | Key 列表（优先于 keys.txt） |
| `KEYS_FILE` | `./keys.txt` | Key 文件路径 |
| `KEY_PICK_STRATEGY` | `priority` | `priority` / `round_robin` / `random` |
| `KEY_COOLDOWN_SECONDS` | `60` | 失败冷却基数（指数退避） |
| `KEY_QUOTA_COOLDOWN_SECONDS` | `120` | 配额类冷却基数（报文带 reset 时间则精确冷却） |
| `KEY_RATELIMIT_COOLDOWN_SECONDS` | `5` | 频率限流冷却基数 |
| `KEY_COOLDOWN_MAX_SECONDS` | `3600` | 冷却封顶 |
| `KEY_MAX_CONSECUTIVE_FAILS` | `3` | 连续 **auth** 失败禁用阈值 |
| `UPSTREAM_PATH_PREFIXES` | `/api/plan/v3,/api/v3,/v3,/v1` | 允许转发的路径前缀 |
| `UPSTREAM_TIMEOUT_SECONDS` | `600` | 上游超时 |

## 测试

```bash
python test_units.py          # 27 项单元测试（Key 池行为、错误分类、真实报文解析）
python test_large_payload.py  # 8 项传输压测（需本地运行，含 60MB 请求/100MB 流式）
```

`mock_upstream.py` 是本地假上游（按 Key 轮转返回 403→429→200），用于无真实 Key 时自测。

## 注意

- 代理不校验下游身份，请勿直接暴露公网；需要时在前面加网关鉴权
- 请遵守各上游服务商的服务条款与配额规则
