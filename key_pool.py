"""通用 LLM API Key 池：分组路由、优先级固定/轮换/随机/窗口轮换选取、失败冷却、禁用恢复，线程安全。"""
import logging
import random
import threading
import time
from dataclasses import dataclass, field

log = logging.getLogger("keypool")

DEFAULT_GROUP = "default"


def mask_key(key: str) -> str:
    """日志/状态里只展示尾 4 位，避免泄露完整 Key。"""
    return "****" + key[-4:] if len(key) >= 4 else "****"


@dataclass
class KeyEntry:
    key: str
    base_url: str = ""  # 该 Key 专属上游地址（含服务路径）；决定上游 URL
    groups: set = field(default_factory=lambda: {DEFAULT_GROUP})  # 所属分组；未打标归 default
    enabled: bool = True  # 连续失败达到阈值后置 False，需恢复
    consecutive_fails: int = 0
    cooldown_until: float = 0.0  # epoch 秒；> now 表示冷却中
    total_success: int = 0
    total_fail: int = 0
    last_error: str = ""
    last_used_at: float = 0.0

    def is_usable(self, now: float) -> bool:
        return self.enabled and self.cooldown_until <= now


@dataclass
class KeyPool:
    keys: list[KeyEntry] = field(default_factory=list)
    strategy: str = "priority"  # priority | round_robin | random | rotation
    cooldown_seconds: float = 60.0
    quota_cooldown_seconds: float = 120.0
    ratelimit_cooldown_seconds: float = 5.0  # 频率限流：几秒即可恢复
    cooldown_max_seconds: float = 3600.0  # 指数退避封顶（1 小时）
    max_consecutive_fails: int = 3
    # rotation 策略窗口触发器（0=关闭该触发器；rotation 时至少一个 > 0）
    window_tokens: int = 0       # 活跃 Key 窗口内累计输入+输出 token 阈值
    window_requests: int = 0     # 活跃 Key 窗口内累计请求数阈值
    window_seconds: float = 0.0  # 活跃 Key 窗口时长阈值
    # rotation 窗口状态，按分组独立记账（每组有自己的活跃 Key 和窗口计数）
    _rot: dict = field(default_factory=dict, repr=False)
    _cursor: int = 0  # round_robin 游标
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    def __post_init__(self):
        if self.strategy == "rotation" and max(self.window_tokens, self.window_requests) <= 0 \
                and self.window_seconds <= 0:
            raise ValueError(
                "rotation strategy requires at least one window trigger > 0 "
                "(ROTATION_WINDOW_TOKENS / ROTATION_WINDOW_REQUESTS / ROTATION_WINDOW_SECONDS)")

    # ---------- 构造 ----------

    @classmethod
    def from_entries(
        cls,
        entries: list[KeyEntry],
        strategy: str = "priority",
        cooldown_seconds: float = 60.0,
        quota_cooldown_seconds: float = 120.0,
        ratelimit_cooldown_seconds: float = 5.0,
        cooldown_max_seconds: float = 3600.0,
        max_consecutive_fails: int = 3,
        window_tokens: int = 0,
        window_requests: int = 0,
        window_seconds: float = 0.0,
    ) -> "KeyPool":
        if not entries:
            raise ValueError("key pool is empty: no valid keys provided")
        if strategy not in ("priority", "round_robin", "random", "rotation"):
            strategy = "priority"
        return cls(
            keys=entries,
            strategy=strategy,
            cooldown_seconds=cooldown_seconds,
            quota_cooldown_seconds=quota_cooldown_seconds,
            ratelimit_cooldown_seconds=ratelimit_cooldown_seconds,
            cooldown_max_seconds=cooldown_max_seconds,
            max_consecutive_fails=max(1, max_consecutive_fails),
            window_tokens=window_tokens,
            window_requests=window_requests,
            window_seconds=window_seconds,
        )

    # ---------- 分组 ----------

    def group_names(self) -> set[str]:
        """池中所有已注册的分组名（来自各 Key 的分组标签）。"""
        return {g for e in self.keys for g in e.groups}

    def _rot_state(self, group: str) -> dict:
        """取指定分组的 rotation 窗口状态，不存在则初始化。"""
        return self._rot.setdefault(
            group, {"active_index": 0, "started_at": time.time(), "requests": 0, "tokens": 0})

    # ---------- 选取 ----------

    def acquire(self, exclude: set[str] | None = None,
                group: str = DEFAULT_GROUP) -> tuple[KeyEntry | None, int]:
        """从指定分组取一个当前可用的 Key。

        exclude: 本次请求已尝试过的 Key 集合——同一请求内不重复尝试同一把
                 （网络失败/服务不匹配不冷却，但要防止同一请求内反复撞同一把）
        group:   目标分组。分组由 keys.txt 第三列标签决定，未打标 Key 归 default 组

        priority:   组内始终取排最前的可用 Key（固定优先级，缓存友好，失败才顺延）
        rotation:   组内窗口轮换（活跃 Key + token/请求数/时间触发器，状态按组独立）
        round_robin: 按游标轮转
        random:     可用集合随机

        返回 (entry, usable_count)。该组没有可用 Key 时返回 (None, 0)。
        """
        with self._lock:
            now = time.time()
            exclude = exclude or set()
            usable = [e for e in self.keys
                      if e.is_usable(now) and e.key not in exclude
                      and group in e.groups]
            if not usable:
                return None, 0
            if self.strategy == "rotation":
                chosen = self._acquire_rotation(usable, now, group)
            elif self.strategy == "random":
                chosen = random.choice(usable)
            elif self.strategy == "priority":
                chosen = usable[0]
            else:
                usable_ids = {id(e) for e in usable}
                n = len(self.keys)
                chosen = None
                for i in range(n):
                    idx = (self._cursor + i) % n
                    if id(self.keys[idx]) in usable_ids:
                        self._cursor = (idx + 1) % n
                        chosen = self.keys[idx]
                        break
                if chosen is None:
                    return None, 0
            chosen.last_used_at = now
            return chosen, len(usable)

    def _acquire_rotation(self, usable: list[KeyEntry], now: float, group: str) -> KeyEntry:
        """rotation 策略（组内独立状态）：流量集中在组内活跃 Key 上，窗口触发器先到先切。

        - 活跃 Key 不可用（冷却/禁用/本请求已试过/不属于该组）→ 立即切组内下一把可用
        - 时间/请求数/token 任一窗口到期 → 切下一把可用
        - 切换（含"只有自己可用"的退化情形）都会重置窗口
        """
        st = self._rot_state(group)
        usable_ids = {id(e) for e in usable}
        n = len(self.keys)
        active_idx = st["active_index"] % n
        reason = None
        if id(self.keys[active_idx]) not in usable_ids:
            reason = "active key unavailable"
        elif self.window_seconds > 0 and now - st["started_at"] >= self.window_seconds:
            reason = "time window expired"
        elif self.window_requests > 0 and st["requests"] >= self.window_requests:
            reason = "request window exhausted"
        elif self.window_tokens > 0 and st["tokens"] >= self.window_tokens:
            reason = "token window exhausted"

        if reason is not None:
            chosen_idx = active_idx
            for i in range(1, n + 1):
                idx = (st["active_index"] + i) % n
                if id(self.keys[idx]) in usable_ids:
                    chosen_idx = idx
                    break
            if chosen_idx != active_idx:
                log.info("rotation[%s]: %s -> switch to key %s",
                         group, reason, mask_key(self.keys[chosen_idx].key))
            else:
                log.info("rotation[%s]: %s -> only this key usable, window reset",
                         group, reason)
            st["active_index"] = chosen_idx
            st["started_at"] = now
            st["requests"] = 0
            st["tokens"] = 0
        chosen = self.keys[st["active_index"]]
        st["requests"] += 1
        return chosen

    # ---------- 成功/失败记账 ----------

    def report_success(self, entry: KeyEntry) -> None:
        with self._lock:
            entry.consecutive_fails = 0
            entry.total_success += 1
            entry.last_error = ""
            entry.cooldown_until = 0.0

    def report_usage(self, group: str, tokens: int) -> None:
        """rotation 窗口的 token 消耗计数（按请求所属分组记账）。"""
        if tokens <= 0:
            return
        with self._lock:
            if self.strategy == "rotation":
                self._rot_state(group)["tokens"] += tokens

    def _cooldown_for(self, kind: str, fails_before: int) -> float:
        """冷却时长：按类别定基数，按连续失败指数退避并封顶。

        - quota:  2 倍 base 起步（上游没给重置时间时的兜底）
        - ratelimit: 5s 起步、退避（5→10→20→…），频率限流恢复快
        - auth/server: base 起步
        """
        if kind == "quota":
            base = self.quota_cooldown_seconds
        elif kind == "ratelimit":
            base = self.ratelimit_cooldown_seconds
        else:
            base = self.cooldown_seconds
        delay = base * (2 ** max(0, fails_before))
        return min(delay, self.cooldown_max_seconds)

    def report_failure(self, entry: KeyEntry, reason: str, kind: str = "auth",
                       cooldown_until: float | None = None) -> bool:
        """记账一次失败。kind: auth | quota | ratelimit | mismatch | server。

        冷却规则：
        - 调用方给出 cooldown_until（如上游报文带明确重置时间）则精确冷却到该时刻
        - 否则按类别基数指数退避（quota 120s / ratelimit 5s / 其他 60s），封顶 1h
        冷却到点 Key 自动重新可用（无需人工干预）。

        禁用规则（需要人工 /pool/recover 才能恢复，因此极度克制）：
        **只有 auth 类失败（401/403 无效凭证）连续超阈值才禁用**——只有它能证明
        Key 本身已坏且短时间内不会自愈。
        - quota/ratelimit 是正常业务波动：冷却但不禁用
        - mismatch（该 Key 的上游不提供请求所需的服务/模型）：不冷却、不计连续
          失败，也不是 Key 的错——换下一把 Key 用它自己的地址重试即可
        - server/5xx 与网络故障可能是上游或本机问题，不能证明 Key 坏，不禁用
        返回是否触发了禁用。
        """
        with self._lock:
            entry.total_fail += 1
            entry.last_error = f"[{kind}] {reason}"[:300]
            if kind == "mismatch":
                log.info(
                    "key %s can't serve this request (mismatch); rotating to next key",
                    mask_key(entry.key),
                )
                return False  # 不冷却、不计连续失败，Key 立即可用于其他请求
            fails_before = entry.consecutive_fails
            entry.consecutive_fails = fails_before + 1
            if cooldown_until is not None:
                delay = max(0.0, cooldown_until - time.time())
            else:
                delay = self._cooldown_for(kind, fails_before)
            entry.cooldown_until = time.time() + delay
            log.info(
                "key %s cooldown %.0fs (fail #%d, kind=%s)%s",
                mask_key(entry.key), delay, fails_before + 1, kind,
                " until reset time" if cooldown_until is not None else "",
            )
            if kind != "auth":
                return False  # 只有无效凭证才可能禁用；其余失败冷却即可
            if entry.consecutive_fails >= self.max_consecutive_fails:
                entry.enabled = False
                log.warning(
                    "key %s DISABLED after %d consecutive auth fails (last: %s)",
                    mask_key(entry.key),
                    entry.consecutive_fails,
                    entry.last_error,
                )
                return True
            return False

    def recover_all(self, key_tail: str | None = None) -> int:
        """手动恢复禁用的 Key；key_tail 指定时只恢复尾号匹配的。返回恢复数量。"""
        with self._lock:
            n = 0
            for e in self.keys:
                if key_tail and not e.key.endswith(key_tail):
                    continue
                if not e.enabled or e.consecutive_fails > 0:
                    e.enabled = True
                    e.consecutive_fails = 0
                    e.cooldown_until = 0.0
                    e.last_error = ""
                    n += 1
            if n:
                log.info("recovered %d key(s)%s", n, f" tail={key_tail}" if key_tail else "")
            return n

    # ---------- 状态 ----------

    def rotation_status(self, group: str = DEFAULT_GROUP) -> dict:
        """指定分组的 rotation 窗口状态；未启用 rotation 时返回带提示的说明。"""
        if self.strategy != "rotation":
            return {
                "enabled": False,
                "hint": "set KEY_PICK_STRATEGY=rotation (plus at least one "
                        "ROTATION_WINDOW_* > 0) to enable windowed rotation",
            }
        with self._lock:
            now = time.time()
            st = self._rot_state(group)
            n = len(self.keys)
            active = self.keys[st["active_index"] % n]
            elapsed = max(0.0, now - st["started_at"])
            out = {
                "enabled": True,
                "group": group,
                "active_key": mask_key(active.key) if group in active.groups else "(unset)",
                "requests": st["requests"],
                "tokens": st["tokens"],
                "elapsed_s": round(elapsed, 1),
            }
            if self.window_seconds > 0:
                out["time_left_s"] = max(0.0, round(self.window_seconds - elapsed, 1))
            if self.window_requests > 0:
                out["requests_left"] = max(0, self.window_requests - st["requests"])
            if self.window_tokens > 0:
                out["tokens_left"] = max(0, self.window_tokens - st["tokens"])
            return out

    def group_info(self, group: str) -> dict:
        """单个分组的概要（Key 清单/可用数/rotation 窗口），供裸组路径 GET 查询。"""
        with self._lock:
            now = time.time()
            members = [e for e in self.keys if group in e.groups]
            info = {
                "group": group,
                "strategy": self.strategy,
                "total": len(members),
                "usable": sum(1 for e in members if e.is_usable(now)),
                "keys": [
                    {
                        "key": mask_key(e.key),
                        "base_url": e.base_url,
                        "enabled": e.enabled,
                        "cooling": e.cooldown_until > now,
                        "total_success": e.total_success,
                        "total_fail": e.total_fail,
                    } for e in members
                ],
            }
            if self.strategy == "rotation":
                info["rotation"] = self.rotation_status(group)
            return info

    def status(self) -> dict:
        with self._lock:
            now = time.time()
            items = []
            for e in self.keys:
                items.append(
                    {
                        "key": mask_key(e.key),
                        "groups": sorted(e.groups),
                        "base_url": e.base_url or "(default)",
                        "enabled": e.enabled,
                        "cooling": e.cooldown_until > now,
                        "cooldown_remaining_s": round(max(0.0, e.cooldown_until - now), 1),
                        "consecutive_fails": e.consecutive_fails,
                        "total_success": e.total_success,
                        "total_fail": e.total_fail,
                        "last_error": e.last_error,
                    }
                )
            usable = sum(1 for e in self.keys if e.is_usable(now))

            groups = {}
            for gname in sorted(self.group_names()):
                members = [e for e in self.keys if gname in e.groups]
                gentry = {
                    "total": len(members),
                    "usable": sum(1 for e in members if e.is_usable(now)),
                }
                if self.strategy == "rotation" and gname in self._rot:
                    st = self._rot[gname]
                    act = self.keys[st["active_index"] % len(self.keys)]
                    gentry["active_key"] = mask_key(act.key)
                    gentry["window_requests"] = st["requests"]
                    gentry["window_tokens"] = st["tokens"]
                groups[gname] = gentry

            return {
                "strategy": self.strategy,
                "total": len(self.keys),
                "usable": usable,
                "disabled": sum(1 for e in self.keys if not e.enabled),
                "cooling": sum(1 for e in self.keys if e.enabled and e.cooldown_until > now),
                "groups": groups,
                "keys": items,
            }
