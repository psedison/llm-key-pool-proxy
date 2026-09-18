# 自动重启守护脚本（Windows / PowerShell）
# 直接前台运行代理：控制台实时输出；代理自身按天写日志到 logs\proxy-YYYYMMDD.log。
# 崩溃/被杀（非零退出码）3 秒后自动重启；Ctrl+C 或退出码 0 视为主动停止。
$ErrorActionPreference = "Continue"
$python = if ($env:PYTHON_BIN) { $env:PYTHON_BIN } else { "python" }

$logDir = Join-Path $PSScriptRoot "logs"
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }
$env:LOG_FILE = Join-Path $logDir ("proxy-{0:yyyyMMdd}.log" -f (Get-Date))

Write-Host "=== llm-key-pool-proxy guard started (stop with Ctrl+C) ===" -ForegroundColor Cyan
Write-Host "=== log file: $env:LOG_FILE ===" -ForegroundColor Cyan

while ($true) {
    & $python proxy_server.py
    $code = $LASTEXITCODE
    if ($code -eq 0) {
        Write-Host "[guard] proxy exited cleanly (code 0), guard stopping" -ForegroundColor Cyan
        break
    }
    Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] proxy exited with code $code, restarting in 3s..." -ForegroundColor Yellow
    Start-Sleep -Seconds 3
}
