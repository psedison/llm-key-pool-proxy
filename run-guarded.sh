#!/usr/bin/env bash
# 自动重启守护脚本（Linux / CentOS / macOS）
# 用法：./run-guarded.sh   或   nohup ./run-guarded.sh >/dev/null 2>&1 &
# 代理进程退出后自动重启（Ctrl+C / kill -TERM 守护进程视为主动停止，不会重启）。
# 日志按天写入 logs/proxy-YYYYMMDD.log，崩溃/被杀后仍有据可查。
set -u
cd "$(dirname "$0")"

PYTHON="${PYTHON_BIN:-python3}"
LOG_DIR="logs"
mkdir -p "$LOG_DIR"

echo "=== llm-key-pool-proxy guard started (stop with Ctrl+C) ==="

STOP=0
on_stop() { STOP=1; echo "[guard] stop signal received"; }
trap on_stop INT TERM

while [ "$STOP" -eq 0 ]; do
    LOG_FILE="$LOG_DIR/proxy-$(date +%Y%m%d).log"
    echo "[$(date '+%F %T')] starting proxy, log: $LOG_FILE"

    # 代理以前台进程运行（PID 记入日志文件便于排查），stdout/stderr 都落盘
    "$PYTHON" proxy_server.py >> "$LOG_FILE" 2>&1 &
    CHILD=$!
    echo "[$(date '+%F %T')] proxy pid=$CHILD"

    # 守护循环：子进程活着就等；子进程退出则记录退出码
    while kill -0 "$CHILD" 2>/dev/null; do
        if [ "$STOP" -eq 1 ]; then
            kill -TERM "$CHILD" 2>/dev/null
        fi
        sleep 2
    done

    wait "$CHILD"
    CODE=$?
    echo "[$(date '+%F %T')] proxy exited with code $CODE"

    if [ "$STOP" -eq 1 ]; then
        echo "[guard] stop requested, guard exiting"
        break
    fi
    echo "[$(date '+%F %T')] restarting in 3s..."
    sleep 3
done
