"""key_pool 与代理纯逻辑的单元测试（无网络）。运行：python test_units.py"""
import calendar
import os
import tempfile
import time
import unittest
import unittest.mock
import urllib.parse

from key_pool import KeyEntry, KeyPool, mask_key
from proxy_server import (
    build_upstream_url,
    resolve_group_and_path,
    classify_failure,
    looks_like_quota_error,
    looks_like_rate_limit_error,
    looks_like_model_mismatch,
    parse_key_line,
    parse_reset_time,
)


class TestParseKeyLine(unittest.TestCase):
    def test_plain_key(self):
        key, url, groups = parse_key_line("ark-abc")
        self.assertEqual((key, url, groups), ("ark-abc", "", frozenset({"default"})))

    def test_key_with_base_url(self):
        key, url, groups = parse_key_line("ark-abc | https://host/api/plan/v3/")
        self.assertEqual((key, url, groups), ("ark-abc", "https://host/api/plan/v3", frozenset({"default"})))

    def test_key_with_groups(self):
        key, url, groups = parse_key_line("ark-abc|https://host/api/plan/v3|ark-plan, ark-coding")
        self.assertEqual(key, "ark-abc")
        self.assertEqual(url, "https://host/api/plan/v3")
        self.assertEqual(groups, frozenset({"ark-plan", "ark-coding"}))

    def test_comment_and_empty(self):
        self.assertIsNone(parse_key_line("# comment"))
        self.assertIsNone(parse_key_line("   "))

    def test_pipe_inside_url_kept(self):
        # 第三个字段（分组）内的竖线原样保留（无损）
        key, url, groups = parse_key_line("k|http://a|x|g1")
        self.assertEqual(key, "k")
        self.assertEqual(url, "http://a")
        self.assertEqual(groups, frozenset({"x|g1"}))


class TestKeyPoolPriority(unittest.TestCase):
    def _pool(self, **kw) -> KeyPool:
        defaults = dict(
            keys=[KeyEntry(key=k) for k in ("k1", "k2", "k3")],
            strategy="priority",
            cooldown_seconds=0.05,
            quota_cooldown_seconds=0.08,
            cooldown_max_seconds=0.2,
            max_consecutive_fails=3,
        )
        defaults.update(kw)
        return KeyPool(**defaults)

    def test_priority_order_and_sticky(self):
        pool = self._pool()
        e1, n = pool.acquire()
        self.assertEqual(e1.key, "k1")
        e2, _ = pool.acquire()
        self.assertEqual(e2.key, "k1", "priority must stay on first key")

    def test_failure_defers_then_recovers_in_place(self):
        pool = self._pool()
        e1, _ = pool.acquire()
        pool.report_failure(e1, "x", "auth")
        e2, _ = pool.acquire()
        self.assertEqual(e2.key, "k2")
        # 位置不变：k1 冷却结束后重新排到最前
        time.sleep(0.06)
        e3, _ = pool.acquire()
        self.assertEqual(e3.key, "k1")

    def test_backoff_doubling_and_cap(self):
        pool = self._pool()
        e1, _ = pool.acquire()
        pool.report_failure(e1, "x", "quota")  # 0.08
        pool.report_failure(e1, "x", "quota")  # 0.16
        remaining = e1.cooldown_until - time.time()
        self.assertGreater(remaining, 0.10)
        pool.report_failure(e1, "x", "quota")  # capped 0.2
        remaining = e1.cooldown_until - time.time()
        self.assertLessEqual(remaining, 0.21)

    def test_disable_and_recover(self):
        pool = self._pool()
        e1, _ = pool.acquire()
        for _ in range(3):
            pool.report_failure(e1, "x", "auth")
        self.assertFalse(e1.enabled)
        self.assertEqual(pool.status()["disabled"], 1)
        self.assertEqual(pool.recover_all(), 1)
        self.assertTrue(e1.enabled)

    def test_recover_by_tail(self):
        pool = self._pool()
        e1, _ = pool.acquire()
        for _ in range(3):
            pool.report_failure(e1, "x", "auth")
        self.assertEqual(pool.recover_all(key_tail="k1"), 1)
        self.assertTrue(e1.enabled)

    def test_success_resets(self):
        pool = self._pool()
        e1, _ = pool.acquire()
        pool.report_failure(e1, "x", "quota")
        pool.report_success(e1)
        self.assertEqual(e1.consecutive_fails, 0)
        self.assertEqual(e1.cooldown_until, 0.0)

    def test_exhaustion_and_round_robin(self):
        pool = self._pool()
        now = time.time()
        for e in pool.keys:
            e.cooldown_until = now + 999
        entry, count = pool.acquire()
        self.assertIsNone(entry)
        self.assertEqual(count, 0)

    def test_round_robin_rotates(self):
        pool = self._pool(strategy="round_robin")
        picked = [pool.acquire()[0].key for _ in range(3)]
        self.assertEqual(sorted(picked), ["k1", "k2", "k3"])

    def test_mask_never_reveals(self):
        self.assertEqual(mask_key("ark-1234567890"), "****7890")
        self.assertEqual(mask_key("ab"), "****")


class TestFailureClassification(unittest.TestCase):
    def test_status_mapping(self):
        self.assertEqual(classify_failure(401, b"{}"), "auth")
        self.assertEqual(classify_failure(403, b"{}"), "auth")
        self.assertEqual(classify_failure(500, b"{}"), "server")

    def test_quota_markers(self):
        self.assertEqual(classify_failure(429, b'{"error":"PlanQuotaExceeded"}'), "quota")
        self.assertEqual(classify_failure(200, b"arrearage"), "quota")
        self.assertEqual(classify_failure(403, "账户额度不足".encode()), "quota")

    def test_looks_like_quota(self):
        self.assertTrue(looks_like_quota_error(b"insufficient balance"))
        self.assertFalse(looks_like_quota_error(b"model not found"))


class TestResetTimeParsing(unittest.TestCase):
    REAL_BODY = (
        b'{"code":"AccountQuotaExceeded","message":"You have exceeded the 5-hour usage quota. '
        b'It will reset at 2026-09-13 22:36:33 +0800 CST. We recommend upgrading your plan '
        b'for more quota, or waiting for the reset. Request id: 0217...","param":"",'
        b'"type":"TooManyRequests"}'
    )

    def test_parse_real_volcano_message(self):
        epoch = parse_reset_time(self.REAL_BODY)
        self.assertIsNotNone(epoch)
        # 2026-09-13 22:36:33 +0800 == 2026-09-13 14:36:33 UTC
        self.assertEqual(epoch, calendar.timegm((2026, 9, 13, 14, 36, 33)))

    def test_unparseable_body_returns_none(self):
        self.assertIsNone(parse_reset_time(b'{"error":"no time here"}'))
        self.assertIsNone(parse_reset_time(b""))

    def test_other_timezone(self):
        body = b'reset at 2026-09-13 08:00:00 -0500 CST.'
        epoch = parse_reset_time(body)
        # 08:00:00 -0500 == 13:00:00 UTC
        self.assertEqual(epoch, calendar.timegm((2026, 9, 13, 13, 0, 0)))


class TestRateLimitClassification(unittest.TestCase):
    REAL_BODY = (
        b'{"code":"AccountRateLimitExceeded","message":"Requests are too frequent. '
        b'Please reduce your request frequency, wait a short moment, and retry your '
        b'request. Request id: 0217...","param":"","type":"TooManyRequests"}'
    )

    def test_real_rate_limit_body(self):
        self.assertEqual(classify_failure(429, self.REAL_BODY), "ratelimit")
        self.assertTrue(looks_like_rate_limit_error(self.REAL_BODY))

    def test_unknown_429_body_never_server(self):
        """真实事故回归（2026-09-15）：未知文案的 429 曾被归为 server，
        server 类计入禁用 → 全池冷却瘫痪。429 必须兜底为 ratelimit。"""
        self.assertEqual(classify_failure(429, b'{"code":"SomethingNew","message":"busy"}'), "ratelimit")
        self.assertEqual(classify_failure(429, b""), "ratelimit")
        self.assertEqual(classify_failure(408, b""), "ratelimit")
        self.assertEqual(classify_failure(425, b""), "ratelimit")

    def test_unknown_429_short_cooldown_never_disables(self):
        pool = KeyPool(
            keys=[KeyEntry(key="k1"), KeyEntry(key="k2")],
            cooldown_seconds=60.0,
            ratelimit_cooldown_seconds=5.0,
        )
        e1, _ = pool.acquire()
        for _ in range(6):
            pool.report_failure(e1, "HTTP 429", "ratelimit")
        self.assertTrue(e1.enabled, "429 fallback must never disable a key")

    def test_rate_limit_not_misclassified_as_server(self):
        # 修复前：此报文不含 quota 关键词 → 被归为 server（60s 冷却 + 计入禁用）
        pool = KeyPool(
            keys=[KeyEntry(key="k1"), KeyEntry(key="k2")],
            cooldown_seconds=60.0,
            ratelimit_cooldown_seconds=5.0,
        )
        e1, _ = pool.acquire()
        for _ in range(5):
            pool.report_failure(e1, "HTTP 429", "ratelimit")
        self.assertTrue(e1.enabled, "ratelimit failures must NEVER disable a key")
        # 冷却短：5s 起步退避 5 次后为 5*2^4=80s，封顶 3600s，但绝不该达到禁用条件
        self.assertLessEqual(e1.consecutive_fails, 5)

    def test_ratelimit_cooldown_shorter_than_quota(self):
        pool = KeyPool(
            keys=[KeyEntry(key="k1")],
            cooldown_seconds=60.0,
            quota_cooldown_seconds=120.0,
            ratelimit_cooldown_seconds=5.0,
        )
        e1, _ = pool.acquire()
        pool.report_failure(e1, "x", "ratelimit")
        rl_cooldown = e1.cooldown_until - time.time()
        e1.cooldown_until = 0.0
        pool.report_failure(e1, "x", "quota")
        q_cooldown = e1.cooldown_until - time.time()
        self.assertLess(rl_cooldown, q_cooldown)


class TestUpstreamUrlBuilding(unittest.TestCase):
    """URL 语义回归：对外前缀只是代理命名空间（剥离），Key 地址全权决定上游服务。
    真实事故 2026-09-17：plan 请求打到 coding Key 拼出双重前缀路径 → SSL EOF。"""

    def test_strips_exposed_prefix_and_uses_key_address(self):
        self.assertEqual(
            build_upstream_url("https://ark.example/api/plan/v3", "/api/plan/v3/chat/completions"),
            "https://ark.example/api/plan/v3/chat/completions")

    def test_same_request_via_coding_key_uses_coding_path(self):
        self.assertEqual(
            build_upstream_url("https://ark.example/api/coding/v3", "/api/plan/v3/chat/completions"),
            "https://ark.example/api/coding/v3/chat/completions")

    def test_other_provider_key_gets_its_own_path(self):
        self.assertEqual(
            build_upstream_url("https://api.deepseek.com/v1", "/api/plan/v3/chat/completions"),
            "https://api.deepseek.com/v1/chat/completions")

    def test_keeps_query_string(self):
        self.assertEqual(
            build_upstream_url("https://h/v1", "/v1/models?a=1"),
            "https://h/v1/models?a=1")

    def test_unknown_prefix_treated_as_suffix(self):
        self.assertEqual(
            build_upstream_url("https://h/api/plan/v3", "/chat/completions"),
            "https://h/api/plan/v3/chat/completions")

    def test_pathless_base_mirrors_full_path(self):
        self.assertEqual(
            build_upstream_url("https://api.deepseek.com", "/api/plan/v3/chat/completions"),
            "https://api.deepseek.com/api/plan/v3/chat/completions")



class TestGroupPathResolution(unittest.TestCase):
    GN = {"gA", "gB", "default"}

    def test_group_path_stripped(self):
        self.assertEqual(
            resolve_group_and_path(self.GN, "/gA/api/v3/chat/completions"),
            ("gA", "/api/v3/chat/completions"))

    def test_bare_group_path(self):
        self.assertEqual(resolve_group_and_path(self.GN, "/gA"), ("gA", "/"))
        self.assertEqual(resolve_group_and_path(self.GN, "/gA/"), ("gA", "/"))

    def test_unknown_first_segment_goes_default_with_path(self):
        self.assertEqual(
            resolve_group_and_path(self.GN, "/api/v3/chat/completions"),
            ("default", "/api/v3/chat/completions"))

    def test_root_goes_default(self):
        self.assertEqual(resolve_group_and_path(self.GN, "/"), ("default", "/"))

    def test_query_param_selects_group(self):
        self.assertEqual(
            resolve_group_and_path(self.GN, "/api/v3/chat/completions?__pool=gB"),
            ("gB", "/api/v3/chat/completions"))

    def test_multi_char_group_name(self):
        self.assertEqual(
            resolve_group_and_path({"ark-plan"}, "/ark-plan/api/v3/chat/completions"),
            ("ark-plan", "/api/v3/chat/completions"))

class TestModelMismatch(unittest.TestCase):
    REAL_BODY = (
        b'{"error":{"code":"UnsupportedModel","message":"The requested model does not '
        b'support the agent plan feature. Please refer to the documentation ..."}}'
    )

    def test_real_unsupported_model_body(self):
        self.assertEqual(classify_failure(404, self.REAL_BODY), "mismatch")
        self.assertTrue(looks_like_model_mismatch(self.REAL_BODY))

    def test_unknown_404_is_mismatch(self):
        self.assertEqual(classify_failure(404, b""), "mismatch")
        self.assertEqual(classify_failure(404, b'{"error":"no page"}'), "mismatch")

    def test_mismatch_never_cools_or_disables(self):
        pool = KeyPool(keys=[KeyEntry(key="k1"), KeyEntry(key="k2")], strategy="priority")
        e1, _ = pool.acquire()
        for _ in range(10):
            pool.report_failure(e1, "UnsupportedModel", "mismatch")
        self.assertTrue(e1.enabled)
        self.assertEqual(e1.cooldown_until, 0.0, "mismatch must not cool the key down")
        self.assertEqual(e1.consecutive_fails, 0, "mismatch must not count toward disable")
        self.assertEqual(e1.total_fail, 10)  # 但记账保留，便于观察


class TestDuplicateKeyDedup(unittest.TestCase):
    def test_duplicate_key_dropped_with_first_wins(self):
        """同 Key 重复行只保留首次出现；9 行含 1 重复 → 8 把（真实案例 2026-09-19）。"""
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False, encoding="utf-8") as f:
            f.write("key-a|https://h1/api/plan/v3\n"
                    "key-b|https://h1/api/plan/v3\n"
                    "key-a|https://h1/api/plan/v3\n")  # 重复
            path = f.name
        try:
            with unittest.mock.patch.dict(os.environ, {"KEYS_FILE": path}, clear=False):
                import importlib
                import config as config_mod
                importlib.reload(config_mod)
                import proxy_server
                importlib.reload(proxy_server)
                entries = proxy_server.load_entries()
        finally:
            os.unlink(path)
        self.assertEqual([e.key for e in entries], ["key-a", "key-b"])

    def test_pathless_base_url_logs_warning(self):
        """纯域名地址易错（端点拼到根上）：load_entries 必须打 WARNING 提示补路径。"""
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False, encoding="utf-8") as f:
            f.write("key-a|https://docode.cc|docode_cc\n")
            path = f.name
        try:
            import logging
            with self.assertLogs("proxy", level="WARNING") as captured:
                with unittest.mock.patch.dict(os.environ, {"KEYS_FILE": path}, clear=False):
                    import importlib
                    import config as config_mod
                    importlib.reload(config_mod)
                    import proxy_server
                    importlib.reload(proxy_server)
                    entries = proxy_server.load_entries()
        finally:
            os.unlink(path)
        self.assertEqual(len(entries), 1)
        self.assertTrue(any("pathless base_url" in m for m in captured.output),
                        f"warning not found in: {captured.output}")

    def test_same_key_different_url_also_deduped(self):
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False, encoding="utf-8") as f:
            f.write("key-a|https://h1/api/plan/v3\n"
                    "key-a|https://h2/api/coding/v3\n")  # 同 Key 不同地址：也去重
            path = f.name
        try:
            with unittest.mock.patch.dict(os.environ, {"KEYS_FILE": path}, clear=False):
                import importlib
                import config as config_mod
                importlib.reload(config_mod)
                import proxy_server
                importlib.reload(proxy_server)
                entries = proxy_server.load_entries()
        finally:
            os.unlink(path)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].base_url, "https://h1/api/plan/v3")  # 首次出现的地址生效


class TestRotationStrategy(unittest.TestCase):
    """窗口轮换策略：活跃 Key + token/请求数/时间三触发器（共享池额度均摊）。"""

    def _pool(self, **kw) -> KeyPool:
        defaults = dict(
            keys=[KeyEntry(key=k) for k in ("k1", "k2", "k3")],
            strategy="rotation",
            window_requests=3,
            window_tokens=0,
            window_seconds=0.0,
        )
        defaults.update(kw)
        return KeyPool(**defaults)

    def test_sticky_within_window(self):
        """窗口内所有请求粘在同一把 Key 上（缓存友好）。"""
        pool = self._pool()
        picks = [pool.acquire()[0].key for _ in range(3)]
        self.assertEqual(picks, ["k1", "k1", "k1"])

    def test_request_window_triggers_rotation(self):
        """第 4 个请求（窗口满 3）切换到下一把。"""
        pool = self._pool()
        picks = [pool.acquire()[0].key for _ in range(4)]
        self.assertEqual(picks, ["k1", "k1", "k1", "k2"])

    def test_token_window_triggers_rotation(self):
        """token 累计达阈值触发切换：3 次 ×40 = 120 ≥ 100 → 第 4 次换 k2。"""
        pool = self._pool(window_requests=0, window_tokens=100)
        keys = []
        for _ in range(4):
            e, _ = pool.acquire()
            keys.append(e.key)
            pool.report_usage("default", 40)
        self.assertEqual(keys, ["k1", "k1", "k1", "k2"])

    def test_time_window_triggers_rotation(self):
        pool = self._pool(window_requests=0, window_seconds=0.05)
        self.assertEqual(pool.acquire()[0].key, "k1")
        time.sleep(0.06)
        self.assertEqual(pool.acquire()[0].key, "k2")

    def test_unavailable_active_advances_and_resets_window(self):
        """活跃 Key 进入冷却 → 立即切下一把，窗口重置（新 Key 拿完整窗口）。"""
        pool = self._pool()
        e1, _ = pool.acquire()
        self.assertEqual(pool._rot["default"]["requests"], 1)
        e1.cooldown_until = time.time() + 999
        e2, _ = pool.acquire()
        self.assertEqual(e2.key, "k2")
        self.assertEqual(pool._rot["default"]["requests"], 1)  # 已重置，而非累加

    def test_excluded_active_advances(self):
        """tried 排除（同请求内已失败）等价于活跃 Key 不可用：换下一把并重置窗口。"""
        pool = self._pool()
        e1, _ = pool.acquire()
        e2, _ = pool.acquire(exclude={e1.key})
        self.assertEqual(e2.key, "k2")
        self.assertEqual(pool._rot["default"]["requests"], 1)

    def test_only_one_usable_stays_and_resets(self):
        """只剩一把可用时退化为粘住它，窗口照常重置不刷屏。"""
        pool = self._pool()
        pool.keys[1].cooldown_until = time.time() + 999
        pool.keys[2].cooldown_until = time.time() + 999
        pool.acquire()  # 先建立 default 组的窗口状态
        pool._rot["default"]["started_at"] = time.time() - 999  # 强制窗口过期
        picks = [pool.acquire()[0].key for _ in range(5)]
        self.assertEqual(set(picks), {"k1"})

    def test_rotation_requires_window_trigger(self):
        with self.assertRaises(ValueError):
            KeyPool(keys=[KeyEntry(key="k1")], strategy="rotation")

    def test_status_reports_window(self):
        pool = self._pool(window_tokens=100)
        pool.acquire()
        pool.report_usage("default", 40)
        st = pool.rotation_status()
        self.assertEqual(st["active_key"], mask_key("k1"))
        self.assertEqual(st["requests"], 1)
        self.assertEqual(st["tokens"], 40)
        self.assertEqual(st["tokens_left"], 60)
        # 其他策略无 rotation 状态
        # 未启用 rotation 时：状态接口自带开启提示，不再返回 null
        disabled = self._pool(strategy="priority").rotation_status()
        self.assertFalse(disabled["enabled"])
        self.assertIn("KEY_PICK_STRATEGY=rotation", disabled["hint"])


class TestQuotaNeverDisables(unittest.TestCase):
    def _pool(self, keys=("k1", "k2")) -> KeyPool:
        return KeyPool(
            keys=[KeyEntry(key=k) for k in keys],
            strategy="priority",
            cooldown_seconds=0.05,
            quota_cooldown_seconds=0.08,
            cooldown_max_seconds=0.2,
            max_consecutive_fails=3,
        )

    def test_many_quota_fails_do_not_disable(self):
        pool = self._pool()
        e1, _ = pool.acquire()
        for _ in range(10):
            pool.report_failure(e1, "quota exceeded", "quota")
        self.assertTrue(e1.enabled, "quota failures must NEVER disable a key")
        self.assertEqual(pool.status()["disabled"], 0)

    def test_auth_fails_still_disable(self):
        pool = self._pool()
        e1, _ = pool.acquire()
        for _ in range(3):
            pool.report_failure(e1, "forbidden", "auth")
        self.assertFalse(e1.enabled)

    def test_server_fails_never_disable(self):
        """5xx/上游故障不证明 Key 坏了，只冷却、不禁用。"""
        pool = self._pool()
        e1, _ = pool.acquire()
        for _ in range(10):
            pool.report_failure(e1, "HTTP 502", "server")
        self.assertTrue(e1.enabled, "server failures must never disable a key")

    def test_network_outage_does_not_accumulate(self):
        """断网场景：连不上上游时不给任何 Key 记账，网络恢复立即满血。

        修复前：3 轮断网请求（每轮间隔一个冷却期）就把全部 Key 禁用且无法自动恢复。
        """
        pool = self._pool(keys=("k1", "k2", "k3"))
        # 模拟断网期间多轮"换 Key 重试"——现在代理对 network 不调用 report_failure
        # （由 proxy_server._relay 的 network 分支保证），这里验证记账接口本身不被误用。
        e1, e2, e3 = pool.keys
        self.assertEqual(pool.report_failure(e1, "conn refused", "server"), False)
        self.assertEqual(pool.report_failure(e2, "conn refused", "server"), False)
        self.assertEqual(pool.report_failure(e3, "conn refused", "server"), False)
        for e in pool.keys:
            self.assertTrue(e.enabled)
            self.assertLess(e.consecutive_fails, pool.max_consecutive_fails)
        # 网络恢复：等冷却（0.05s 级）过去后下一个请求立即可用，无需 recover
        time.sleep(0.1)
        entry, usable = pool.acquire()
        self.assertIsNotNone(entry)

    def test_reset_time_cooldown_used_verbatim(self):
        pool = self._pool()
        e1, _ = pool.acquire()
        future = time.time() + 3600  # 上游说 1 小时后重置
        pool.report_failure(e1, "quota", "quota", cooldown_until=future)
        self.assertAlmostEqual(e1.cooldown_until, future, delta=1)
        self.assertTrue(e1.enabled)


if __name__ == "__main__":
    unittest.main(verbosity=2)
