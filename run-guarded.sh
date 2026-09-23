#!/usr/bin/env bash
# 自动重启守护脚本（Linux / CentOS / macOS）
# 直接前台运行代理：控制台实时输出；代理自身按天写日志到 logs/proxy-YYYYMMDD.log。
# 崩溃/被杀（非零退出码）3 秒后自动重启；Ctrl+C 或退出码 0 视为主动停止。
# 后台运行：nohup ./run-guarded.sh >/dev/null 2>&1 &   停止：kill <守护脚本PID>（会优雅停掉代理）
set -u
cd "$(dirname "$0")"

PYTHON="${PYTHON_BIN:-python3}"
export PYTHONNOUSERSITE=1  # 代理仅用标准库；跳过用户站点目录，免疫残留 .pth 导致的启动崩溃
mkdir -p logs
export LOG_FILE="logs/proxy-$(date +%Y%m%d).log"

echo "=== llm-key-pool-proxy guard started (stop with Ctrl+C) ==="
echo "=== log file: $LOG_FILE ==="

STOP=0
CHILD=""
on_stop() {
    STOP=1
    echo "[guard] stop signal received"
    # 优雅停掉代理（它的 SIGTERM 处理会打日志再退出）；INT 场景 Ctrl+C 已直达代理，重复 kill 无害
    [ -n "$CHILD" ] && kill -TERM "$CHILD" 2>/dev/null
}
trap on_stop INT TERM

while [ "$STOP" -eq 0 ]; do
    "$PYTHON" proxy_server.py &
    CHILD=$!
    echo "[guard] proxy pid=$CHILD"
    wait "$CHILD"
    CODE=$?
    echo "[guard] proxy exited with code $CODE at $(date '+%F %T')"

    if [ "$CODE" -eq 0 ]; then
        echo "[guard] clean exit, guard stopping"
        break
    fi
    if [ "$STOP" -eq 1 ]; then
        echo "[guard] stop requested, guard exiting"
        break
    fi
    echo "[guard] restarting in 3s..."
    sleep 3
done
